#!/bin/bash
#
# LetsEncrypt renewal script for Headwind MDM
#

# Set this parameter to 1 if you're redirecting port 80 to 8080 to be able to run Headwind MDM on port 80
HTTP_REDIRECT=0
DOMAIN=your-domain.com
TOMCAT_HOME=$(ls -d /var/lib/tomcat* | tail -n1)
TOMCAT_SERVICE=$(echo $TOMCAT_HOME | awk '{n=split($1,A,"/"); print A[n]}')

if [ "$DOMAIN" = "your-domain.com" ]; then
    echo "Please edit this script and update HTTP_REDIRECT and DOMAIN variables!"
    exit 1
fi

# Written by hmdm_install.sh. Single source of the certificate name, its lineage
# path and the Tomcat account -- the deploy hook reads the same file, so the two
# cannot drift apart.
HOOK_ENV=/etc/hmdm/tls/hook.env
if [ ! -r "$HOOK_ENV" ]; then
    echo "$HOOK_ENV is missing or unreadable."
    echo "Run hmdm_install.sh to set up the certbot deploy hook before renewing."
    exit 1
fi
. "$HOOK_ENV"                     # CERT_NAME, TOMCAT_USER, TOMCAT_GROUP, HMDM_LINEAGE

if [ -z "$CERT_NAME" ] || [ -z "$HMDM_LINEAGE" ]; then
    echo "$HOOK_ENV does not define CERT_NAME and HMDM_LINEAGE"
    exit 1
fi

# The lineage must agree with the name, or hook.env has been edited inconsistently.
if [ "$HMDM_LINEAGE" != "/etc/letsencrypt/live/$CERT_NAME" ]; then
    echo "hook.env inconsistent: HMDM_LINEAGE=$HMDM_LINEAGE, CERT_NAME=$CERT_NAME"
    exit 1
fi

DEPLOY_HOOK=/etc/letsencrypt/renewal-hooks/deploy/hmdm-tls
if [ ! -x "$DEPLOY_HOOK" ]; then
    echo "$DEPLOY_HOOK is missing or not executable."
    echo "Run hmdm_install.sh to install the certbot deploy hook before renewing."
    exit 1
fi

# --- port-80 redirect ------------------------------------------------------
# The redirect has to come down for certbot's standalone challenge and go back up
# afterwards. Both directions are checked: a silent failure to remove it makes the
# challenge fail, and a blind re-add either duplicates the rule or invents one on a
# host that never had it.
REDIRECT_ARGS="PREROUTING -t nat -p tcp -m tcp --dport 80 -j REDIRECT --to-ports 8080"
REDIRECT_REMOVED=0

do_restore_redirect() {             # returns a status; never exits
    [ "$REDIRECT_REMOVED" = "1" ] || return 0
    if /sbin/iptables -A $REDIRECT_ARGS; then
        REDIRECT_REMOVED=0
        return 0
    fi
    echo "FATAL: could not restore the port-80 redirect -- restore it by hand:" >&2
    echo "  /sbin/iptables -A $REDIRECT_ARGS" >&2
    return 1
}

# INT/TERM must not share the EXIT handler: inside a signal handler $? can be 0,
# so an interrupted renewal would restore the redirect and exit 0 -- reporting
# success without ever having published a certificate. Give them explicit
# non-zero exits, which then fire the EXIT trap.
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'rc=$?; do_restore_redirect || rc=1; exit $rc' EXIT

if [ "$HTTP_REDIRECT" = "1" ]; then
    if /sbin/iptables -C $REDIRECT_ARGS 2>/dev/null; then
        if /sbin/iptables -D $REDIRECT_ARGS; then
            REDIRECT_REMOVED=1
        else
            echo "FATAL: port-80 redirect present but could not be removed;" >&2
            echo "       the standalone challenge would fail -- aborting" >&2
            exit 1
        fi
    fi
    # Rule genuinely absent: nothing removed, so nothing to restore.
fi

# --cert-name pins the lineage so the live path is known before certbot runs.
# --no-directory-hooks guarantees exactly one publication: certbot runs the
# renewal-hooks/deploy/ directory for certonly too, not only for renew, so without
# this flag the hook would fire once from certbot and once from the call below.
certbot certonly --agree-tos --no-eff-email --standalone --force-renewal \
        --cert-name "$CERT_NAME" --no-directory-hooks -d "$DOMAIN"
CERTBOT_STATUS=$?

# Restore before any exit path, and check that it worked.
if ! do_restore_redirect; then
    exit 1
fi

# Certbot's status is checked, which is what the old TODO here was about: without
# it a failed renewal leaves the PREVIOUS lineage directory in place, so the
# existence check below passes and the hook republishes stale material as if it
# were fresh.
if [ "$CERTBOT_STATUS" -ne 0 ]; then
    echo "certbot failed (status $CERTBOT_STATUS) -- NOT publishing; previous certificate left in place"
    exit "$CERTBOT_STATUS"
fi

if [ ! -d "$HMDM_LINEAGE" ]; then
    echo "expected lineage $HMDM_LINEAGE missing after certonly"
    exit 1
fi

# Exactly one publication, with the lineage passed explicitly. The status is
# checked because the hook validates the key/cert pair and can legitimately refuse
# to publish, and more lines follow in this script.
if ! RENEWED_LINEAGE="$HMDM_LINEAGE" RENEWED_DOMAINS="$DOMAIN" "$DEPLOY_HOOK"; then
    echo "FATAL: deploy hook failed -- certificate issued but NOT published;" >&2
    echo "       /etc/hmdm/tls/current still points at the previous generation" >&2
    exit 1
fi

ENCRYPTION=RSA
CERTBOT_VERSION=`certbot --version | awk '{print $2}' | awk '{n=split($1,A,"."); print A[1]}'`
if [ "$CERTBOT_VERSION" != "" ] && [ "$CERTBOT_VERSION" -ge "2" ]; then
    # In certbot 2, default encryption is ECDSA so we need to adjust it in Tomcat config
    ENCRYPTION=EC
fi

echo "The certificates have been published here: /etc/hmdm/tls/current/"
echo "Please add / uncomment the following section in $TOMCAT_HOME/conf/server.xml:"
echo "<Connector port=\"8443\" protocol=\"org.apache.coyote.http11.Http11NioProtocol\""
echo "           maxThreads=\"150\" SSLEnabled=\"true\">"
echo "    <SSLHostConfig>"
echo "        <Certificate certificateKeyFile=\"/etc/hmdm/tls/current/privkey.pem\""
echo "                     certificateFile=\"/etc/hmdm/tls/current/cert.pem\""
echo "                     certificateChainFile=\"/etc/hmdm/tls/current/chain.pem\""
echo "                     type=\"$ENCRYPTION\" />"
echo "    </SSLHostConfig>"
echo "</Connector>"

# This line is required when you refresh the certificates because Tomcat needs
# to be restarted to load a new certificate.
# Here we assume the service has the same name as the Tomcat directory
# (e.g. tomcat9)
/usr/sbin/service $TOMCAT_SERVICE restart

#!/bin/sh
#
# Headwind MDM certbot deploy hook.
#
# Installed by hmdm_install.sh to /etc/letsencrypt/renewal-hooks/deploy/hmdm-tls.
# Certbot runs every executable in that directory ONCE PER SUCCESSFULLY RENEWED
# CERTIFICATE ON THE HOST -- not once per certbot invocation -- so the first thing
# this script does is check that the renewal it was handed is actually ours.
#
# What it does, in order:
#   1. stage the new PEMs into a fresh generation directory
#   2. verify the private key and the certificate are a matching pair
#   3. apply root-owned, Tomcat-group-readable ownership
#   4. publish atomically by swapping a single symlink
#   5. rewrite the Artemis pemcfg so the running broker notices the new material
#   6. prune old generations, keeping the previous one for rollback
#
# Any failure before step 4 leaves the previous generation published and intact.
#
set -eu

# Written by the installer. Defines HMDM_LINEAGE (the exact certbot live path),
# TOMCAT_USER and TOMCAT_GROUP. Root-only: it names the certificate lineage.
ENV_FILE=/etc/hmdm/tls/hook.env
[ -r "$ENV_FILE" ] || { echo "hmdm-tls: $ENV_FILE missing" >&2; exit 1; }
. "$ENV_FILE"
: "${HMDM_LINEAGE:?hook.env must define HMDM_LINEAGE}"
: "${TOMCAT_GROUP:?hook.env must define TOMCAT_GROUP}"
: "${TOMCAT_USER:?hook.env must define TOMCAT_USER}"

TLS_DIR=/etc/hmdm/tls
PEMCFG=/var/lib/hmdm/mqtt/mqtt-broker.pemcfg
PEM_FILES="privkey.pem fullchain.pem cert.pem chain.pem"

# Ignore renewals for any lineage that is not ours. Exact match, no globbing:
# on a host serving several domains an unrelated renewal must not overwrite
# HMDM's published certificate with a stranger's.
[ "${RENEWED_LINEAGE:-}" = "$HMDM_LINEAGE" ] || exit 0

log() { echo "hmdm-tls: $*"; }
fail() { echo "hmdm-tls: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Stage
#
# The "-$$" is load-bearing: the timestamp has one-second resolution, so two hook
# runs inside the same second would otherwise target the same directory and one
# would clobber the other's half-copied contents. letsencrypt-ssl.sh passes
# --no-directory-hooks so our own path cannot double-fire, but "certbot renew"
# from certbot's own systemd timer is outside our control.
# ---------------------------------------------------------------------------
STAMP="$(date -u +%Y%m%dT%H%M%SZ)-$$"
GEN_DIR="$TLS_DIR/gen-$STAMP"

# Plain mkdir, deliberately not -p: this must fail if the directory somehow exists.
mkdir "$GEN_DIR" || fail "could not create $GEN_DIR"

# From here until the symlink swap, any failure removes the half-built generation
# and leaves the previously published one untouched.
cleanup_staging() {
    rm -rf "$GEN_DIR"
}
trap 'cleanup_staging' EXIT

for f in $PEM_FILES; do
    [ -r "$RENEWED_LINEAGE/$f" ] || fail "$RENEWED_LINEAGE/$f is missing or unreadable"
    cp "$RENEWED_LINEAGE/$f" "$GEN_DIR/$f" || fail "could not copy $f"
done

# ---------------------------------------------------------------------------
# 2. Validate the pair BEFORE publishing
#
# Publishing a key that does not match its certificate takes the site down as
# surely as publishing nothing, and it does so silently until the next handshake.
# ---------------------------------------------------------------------------
key_pub="$(openssl pkey -in "$GEN_DIR/privkey.pem" -pubout 2>/dev/null)" \
    || fail "could not read the private key -- refusing to publish"
cert_pub="$(openssl x509 -in "$GEN_DIR/cert.pem" -pubkey -noout 2>/dev/null)" \
    || fail "could not read the certificate -- refusing to publish"

if [ "$key_pub" != "$cert_pub" ]; then
    fail "private key does not match the certificate -- refusing to publish; previous generation left in place"
fi

# ---------------------------------------------------------------------------
# 3. Ownership: root-owned, Tomcat-group-readable.
#
# Never Tomcat-owned: a Tomcat-owned private key could be rewritten by a
# compromised application.
# ---------------------------------------------------------------------------
chown root:"$TOMCAT_GROUP" "$GEN_DIR" || fail "could not chown $GEN_DIR"
chmod 0750 "$GEN_DIR" || fail "could not chmod $GEN_DIR"
for f in $PEM_FILES; do
    chown root:"$TOMCAT_GROUP" "$GEN_DIR/$f" || fail "could not chown $f"
    chmod 0640 "$GEN_DIR/$f" || fail "could not chmod $f"
done

# ---------------------------------------------------------------------------
# 4. Publish atomically
#
# Four independent mv operations cannot atomically publish a key/cert set: a
# reader landing between them observes a mismatched key and chain. Publish the
# whole directory, then swap one pointer with a single rename(2).
# ---------------------------------------------------------------------------
ln -sfn "gen-$STAMP" "$TLS_DIR/.current.tmp" || fail "could not stage the current symlink"
mv -T "$TLS_DIR/.current.tmp" "$TLS_DIR/current" || {
    rm -f "$TLS_DIR/.current.tmp"
    fail "could not publish the current symlink"
}

# Published. The staged directory is now live, so stop treating it as scratch.
trap - EXIT
log "published gen-$STAMP for ${RENEWED_DOMAINS:-$HMDM_LINEAGE}"

# ---------------------------------------------------------------------------
# 5. Signal Artemis
#
# The broker watches the pemcfg file, not the PEMs. Swapping the "current"
# symlink changes no inode that Artemis has open, so rewriting this file is what
# tells it to reload. Written to a temp file in the same directory and moved into
# place so the broker never reads a half-written config.
# ---------------------------------------------------------------------------
PEMCFG_DIR=$(dirname "$PEMCFG")
if [ -d "$PEMCFG_DIR" ]; then
    PEMCFG_TMP="$PEMCFG_DIR/.mqtt-broker.pemcfg.$$"
    {
        echo "source.key=$TLS_DIR/current/privkey.pem"
        echo "source.cert=$TLS_DIR/current/fullchain.pem"
    } > "$PEMCFG_TMP" || fail "could not write $PEMCFG_TMP"
    # Tomcat rewrites this file itself at startup, so leave it Tomcat-owned.
    chown "$TOMCAT_USER":"$TOMCAT_GROUP" "$PEMCFG_TMP" || fail "could not chown $PEMCFG_TMP"
    chmod 0640 "$PEMCFG_TMP" || fail "could not chmod $PEMCFG_TMP"
    mv -f "$PEMCFG_TMP" "$PEMCFG" || fail "could not publish $PEMCFG"
    log "rewrote $PEMCFG"
else
    # Not fatal: the certificate is published and Tomcat will pick it up on its
    # next start. Only the live MQTT reload is lost.
    echo "hmdm-tls: WARNING $PEMCFG_DIR does not exist; MQTT will not reload until Tomcat restarts" >&2
fi

# ---------------------------------------------------------------------------
# 6. Prune, keeping the current generation and the one before it
#
# The previous generation is what a rollback needs; anything older is dead weight
# holding a private key on disk.
# ---------------------------------------------------------------------------
KEEP=2
# shellcheck disable=SC2012 # names are generated by this script; no odd characters
ls -1d "$TLS_DIR"/gen-* 2>/dev/null | sort -r | tail -n +$((KEEP + 1)) | while read -r old; do
    # Never prune the generation just published, whatever the sort produced.
    if [ "$old" = "$GEN_DIR" ]; then
        continue
    fi
    if rm -rf "$old"; then
        log "pruned $(basename "$old")"
    else
        echo "hmdm-tls: WARNING could not prune $old" >&2
    fi
done

exit 0

/*
 * Headwind MDM: Open Source Android MDM Software https://h-mdm.com
 *
 * Copyright (C) 2019 Headwind Solutions LLC (https://h-mdm.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations
 * under the License.
 */

package com.hmdm.plugins;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * <p>Shared harness for the tenant-isolation integration tests: one PostgreSQL container, the Liquibase bootstrap, and the small SQL helpers the
 * fixtures need.</p>
 *
 * <p>Declared once and referenced by both suites. If the two suites named the image separately they could drift apart, and would then not be testing
 * the same database.</p>
 */
public final class TenantIsolationSupport {

    /**
     * System property carrying the container image. There is deliberately <em>no default</em>.
     *
     * <p>A floating tag would let the image change between the run that produced the gate evidence and any later run, which defeats the point of
     * recording that evidence. The value must therefore be digest-pinned, and {@link #resolveImage()} refuses anything else rather than silently
     * accepting a tag.</p>
     *
     * <p>Resolve the digest with:</p>
     *
     * <pre>
     * docker buildx imagetools inspect postgres:16.4-alpine3.20
     * </pre>
     *
     * <p>then run:</p>
     *
     * <pre>
     * mvn verify -Pit -Dhmdm.it.pgImage=postgres:16.4-alpine3.20@sha256:&lt;digest&gt;
     * </pre>
     */
    public static final String IMAGE_PROPERTY = "hmdm.it.pgImage";

    /**
     * The root changelog uses three contexts: {@code common}, {@code shared} and {@code private}. {@code shared} and {@code private} are mutually
     * exclusive multi-tenancy scenarios, and the {@code shared} changeset is the one that creates the DEFAULT and ADMIN customers. Tenant-isolation
     * tests need more than one customer to exist, so the bootstrap runs {@code common,shared} — running {@code common} alone would leave a
     * single-tenant database in which these tests cannot express their premise.
     */
    private static final String CONTEXTS = "common,shared";

    /** Applied in order. Each is resolvable from the server module's test classpath. */
    private static final String[] CHANGELOGS = {
                    "liquibase/db.changelog.xml", // server
                    "liquibase/deviceinfo.changelog.xml", // deviceinfo plugin
                    "liquibase/devicelog.postgres.changelog.xml", // devicelog-postgres plugin
    };

    private TenantIsolationSupport() {}

    /**
     * Reads the pinned image from {@link #IMAGE_PROPERTY} and rejects anything that is not digest-pinned.
     *
     * @throws IllegalStateException with an actionable message when the property is absent or carries a floating tag
     */
    public static DockerImageName resolveImage() {
        String image = System.getProperty(IMAGE_PROPERTY);
        if (image == null || image.isBlank()) {
            throw new IllegalStateException("Set -D" + IMAGE_PROPERTY + "=postgres:16.4-alpine3.20@sha256:<digest>. "
                    + "Resolve the digest with: docker buildx imagetools inspect postgres:16.4-alpine3.20");
        }
        if (!image.contains("@sha256:")) {
            throw new IllegalStateException("-D" + IMAGE_PROPERTY + " must be digest-pinned (…@sha256:…), got: " + image
                    + ". A floating tag lets the image drift between the gate run and any later run.");
        }
        // asCompatibleSubstituteFor: the digest-pinned reference is not recognised as the postgres image by name alone.
        return DockerImageName.parse(image).asCompatibleSubstituteFor("postgres");
    }

    /** Note: PostgreSQLContainer is NOT generic in Testcontainers 2.x, unlike the 1.x {@code PostgreSQLContainer<?>}. */
    public static PostgreSQLContainer newContainer() {
        return new PostgreSQLContainer(resolveImage());
    }

    public static Connection connect(PostgreSQLContainer pg) throws SQLException {
        return DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    }

    /**
     * Applies the root changelog and both plugin changelogs. Each changelog gets its own {@link Liquibase} instance because each carries its own
     * DATABASECHANGELOG identity.
     */
    public static void bootstrapSchema(Connection connection) throws Exception {
        Database database =
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
        for (String changelog : CHANGELOGS) {
            try (Liquibase liquibase = new Liquibase(changelog, new ClassLoaderResourceAccessor(), database)) {
                liquibase.update(new Contexts(CONTEXTS), new LabelExpression());
            }
        }
    }

    public static void exec(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /** Runs a single-value query and returns the first column of the first row as an int. */
    public static int scalarInt(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                throw new SQLException("query returned no rows: " + sql);
            }
            return rs.getInt(1);
        }
    }

    /**
     * Creates a customer and returns its id. {@code name} and {@code filesDir} both carry UNIQUE constraints, so they are derived from the caller's
     * label rather than fixed.
     */
    public static int createCustomer(Connection connection, String label) throws SQLException {
        return scalarInt(
                connection,
                "INSERT INTO customers (name, description, master, filesDir) VALUES ("
                        + quote(label) + ", " + quote("tenant isolation IT " + label) + ", FALSE, " + quote(label)
                        + ") RETURNING id");
    }

    /**
     * Creates a device owned by {@code customerId} and returns its id.
     *
     * <p>Device ids are globally unique primary keys and {@code devices.number} is UNIQUE, so callers must give each customer distinct device numbers
     * — overlapping ids or numbers would make the isolation assertions meaningless.</p>
     */
    public static int createDevice(Connection connection, int customerId, String number) throws SQLException {
        return scalarInt(
                connection,
                "INSERT INTO devices (number, description, lastUpdate, customerId) VALUES ("
                        + quote(number) + ", " + quote("tenant isolation IT") + ", 0, " + customerId + ") RETURNING id");
    }

    /**
     * Ensures exactly one settings row for this customer, then asserts it.
     *
     * <p>The two settings tables are asymmetric, which is why a blind insert is wrong: {@code
     * plugin_deviceinfo_settings} has UNIQUE(customerId) and is never populated by its changelog, while {@code
     * plugin_devicelog_settings} has no unique constraint and IS partly populated by one. A second insert into the latter creates a silent duplicate,
     * and the purge statements' scalar subquery then raises "more than one row returned by a subquery used as an expression" — failing identically
     * before and after the fix.</p>
     *
     * <p>Both purge queries derive their cutoff from that scalar subquery, so with no settings row at all the subquery yields NULL, {@code ts < NULL}
     * is NULL, and nothing is deleted — by the old mapper and the new one alike, so the isolation assertion would pass vacuously.</p>
     */
    public static void setRetentionDays(Connection connection, String table, String periodColumn, int customerId, int days)
            throws SQLException {
        exec(connection, "DELETE FROM " + table + " WHERE customerId = " + customerId);
        exec(
                connection,
                "INSERT INTO " + table + " (customerId, " + periodColumn + ") VALUES (" + customerId + ", " + days + ")");
        int rows = scalarInt(connection, "SELECT count(*) FROM " + table + " WHERE customerId = " + customerId);
        if (rows != 1) {
            throw new IllegalStateException(
                    "expected exactly one " + table + " row for customer " + customerId + ", found " + rows);
        }
    }

    /** Epoch milliseconds for "n days ago", matching the millisecond timestamps both plugins store. */
    public static long daysAgoMillis(int days) {
        return System.currentTimeMillis() - (days * 86_400_000L);
    }

    private static String quote(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}

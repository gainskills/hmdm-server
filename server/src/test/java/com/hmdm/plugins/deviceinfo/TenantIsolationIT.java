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

package com.hmdm.plugins.deviceinfo;

import static com.hmdm.plugins.TenantIsolationSupport.bootstrapSchema;
import static com.hmdm.plugins.TenantIsolationSupport.connect;
import static com.hmdm.plugins.TenantIsolationSupport.createCustomer;
import static com.hmdm.plugins.TenantIsolationSupport.createDevice;
import static com.hmdm.plugins.TenantIsolationSupport.daysAgoMillis;
import static com.hmdm.plugins.TenantIsolationSupport.exec;
import static com.hmdm.plugins.TenantIsolationSupport.newContainer;
import static com.hmdm.plugins.TenantIsolationSupport.scalarInt;
import static com.hmdm.plugins.TenantIsolationSupport.setRetentionDays;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hmdm.plugins.deviceinfo.persistence.mapper.DeviceInfoMapper;
import java.sql.Connection;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * <p>Proves that the Device Info purge deletes only the calling customer's records.</p>
 *
 * <p>Purge isolation only. A Device Info <em>search</em> test is deliberately absent from this suite: it would pass both before and after the fix, so
 * it would prove nothing.</p>
 *
 * <p>Manual container lifecycle rather than {@code @Testcontainers}/{@code @Container}: this branch is on JUnit 6, and that extension targets the
 * JUnit 5 extension API.</p>
 */
class TenantIsolationIT {

    private static PostgreSQLContainer pg;
    private static Connection connection;
    private static SqlSessionFactory sessionFactory;

    private static int customerA;
    private static int customerB;
    private static int deviceA;
    private static int deviceB;

    @BeforeAll
    static void startContainer() throws Exception {
        pg = newContainer();
        pg.start();
        connection = connect(pg);
        bootstrapSchema(connection);
        sessionFactory = buildSessionFactory();
        seedFixture();
    }

    @AfterAll
    static void stopContainer() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (pg != null) {
            pg.stop();
        }
    }

    private static SqlSessionFactory buildSessionFactory() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(pg.getJdbcUrl());
        dataSource.setUser(pg.getUsername());
        dataSource.setPassword(pg.getPassword());

        Configuration configuration =
                new Configuration(new Environment("it", new JdbcTransactionFactory(), dataSource));
        // Aliases before the mapper: DeviceInfoMapper.xml is parsed when the mapper is added, and a
        // programmatic Configuration resolves type names at parse time with no deferral.
        configuration.getTypeAliasRegistry().registerAliases("com.hmdm.plugins.deviceinfo.persistence.domain");
        configuration.getTypeAliasRegistry().registerAliases("com.hmdm.plugins.deviceinfo.rest.json");
        configuration.addMapper(DeviceInfoMapper.class);
        return new SqlSessionFactoryBuilder().build(configuration);
    }

    /**
     * Customer A keeps data for 1 day. Both A's and B's records are older than that cutoff, so a purge run as A deletes A's record either way — that
     * assertion is a positive control and proves nothing on its own.
     *
     * <p><b>"B's record is retained" is the assertion that detects the fix.</b></p>
     */
    private static void seedFixture() throws Exception {
        customerA = createCustomer(connection, "tenantA");
        customerB = createCustomer(connection, "tenantB");

        // Distinct device numbers per customer: devices.number is UNIQUE and ids are global.
        deviceA = createDevice(connection, customerA, "IT-A-0001");
        deviceB = createDevice(connection, customerB, "IT-B-0001");

        setRetentionDays(connection, "plugin_deviceinfo_settings", "dataPreservePeriod", customerA, 1);
        setRetentionDays(connection, "plugin_deviceinfo_settings", "dataPreservePeriod", customerB, 365);

        long old = daysAgoMillis(30);
        insertRecord(customerA, deviceA, old);
        insertRecord(customerB, deviceB, old);

        assertEquals(1, countFor(customerA), "fixture: customer A should start with one record");
        assertEquals(1, countFor(customerB), "fixture: customer B should start with one record");
    }

    private static void insertRecord(int customerId, int deviceId, long ts) throws Exception {
        exec(
                connection,
                "INSERT INTO plugin_deviceinfo_deviceParams (deviceId, customerId, ts) VALUES (" + deviceId + ", "
                        + customerId + ", " + ts + ")");
    }

    private static int countFor(int customerId) throws Exception {
        return scalarInt(
                connection, "SELECT count(*) FROM plugin_deviceinfo_deviceParams WHERE customerId = " + customerId);
    }

    @Test
    void purgeAsOneCustomerLeavesTheOtherCustomersRecords() throws Exception {
        try (SqlSession session = sessionFactory.openSession(true)) {
            session.getMapper(DeviceInfoMapper.class).purgeDeviceInfoRecords(customerA);
        }

        // Positive control: passes before and after the fix.
        assertEquals(0, countFor(customerA), "customer A's expired record should have been purged");

        // The assertion that detects the fix. Pre-fix the DELETE has no customerId predicate of its own,
        // so it removes every customer's records older than A's cutoff.
        assertEquals(
                1,
                countFor(customerB),
                "customer B's record was deleted by a purge run as customer A — the purge is not tenant-scoped");
    }

    @Test
    void purgeRespectsTheCallingCustomersOwnRetentionPeriod() throws Exception {
        // B keeps data for 365 days and its record is 30 days old, so purging as B must delete nothing.
        try (SqlSession session = sessionFactory.openSession(true)) {
            session.getMapper(DeviceInfoMapper.class).purgeDeviceInfoRecords(customerB);
        }
        assertEquals(1, countFor(customerB), "customer B's record is inside B's own retention period");
    }
}

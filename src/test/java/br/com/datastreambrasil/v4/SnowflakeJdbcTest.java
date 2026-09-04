package br.com.datastreambrasil.v4;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnowflakeJdbcTest {

    private static final String URL =
            "jdbc:snowflake://acme.snowflakecomputing.com?schema=LZ_IH&db=DATB200D&CLIENT_SESSION_KEEP_ALIVE=TRUE";

    @Test
    void readsDatabaseAndSchemaFromTheJdbcUrl() {
        assertEquals("DATB200D", SnowflakeJdbc.urlParam(URL, "db", "database").orElseThrow());
        assertEquals("LZ_IH", SnowflakeJdbc.urlParam(URL, "schema").orElseThrow());
    }

    @Test
    void acceptsTheDriverSynonymsAndIgnoresCase() {
        var url = "jdbc:snowflake://acme.snowflakecomputing.com?DATABASE=DATB200D&Schema=LZ_IH";

        assertEquals("DATB200D", SnowflakeJdbc.urlParam(url, "db", "database").orElseThrow());
        assertEquals("LZ_IH", SnowflakeJdbc.urlParam(url, "schema").orElseThrow());
    }

    @Test
    void decodesEscapedValues() {
        var url = "jdbc:snowflake://acme.snowflakecomputing.com?db=MY%20DB";

        assertEquals("MY DB", SnowflakeJdbc.urlParam(url, "db").orElseThrow());
    }

    @Test
    void derivesTheAccountFromTheHostLikeTheDriverDoes() {
        assertEquals("acme", SnowflakeJdbc.accountFrom(URL).orElseThrow());

        // the first host label wins, so region-qualified and PrivateLink hosts still resolve
        assertEquals("acme", SnowflakeJdbc.accountFrom(
                "jdbc:snowflake://acme.us-east-1.snowflakecomputing.com?db=X").orElseThrow());
        assertEquals("acme", SnowflakeJdbc.accountFrom(
                "jdbc:snowflake://acme.privatelink.snowflakecomputing.com?db=X").orElseThrow());

        // org-account identifiers keep their hyphen
        assertEquals("myorg-myaccount", SnowflakeJdbc.accountFrom(
                "jdbc:snowflake://myorg-myaccount.snowflakecomputing.com").orElseThrow());
    }

    @Test
    void anExplicitAccountParameterWinsOverTheHost() {
        assertEquals("other", SnowflakeJdbc.accountFrom(
                "jdbc:snowflake://acme.snowflakecomputing.com?account=other").orElseThrow());
    }

    @Test
    void extractsTheHostForTheStreamingEndpoint() {
        assertEquals("acme.snowflakecomputing.com", SnowflakeJdbc.hostFrom(URL).orElseThrow());
        assertEquals("acme.privatelink.snowflakecomputing.com", SnowflakeJdbc.hostFrom(
                "jdbc:snowflake://acme.privatelink.snowflakecomputing.com:443/?db=X").orElseThrow());
        assertEquals("acme.snowflakecomputing.com", SnowflakeJdbc.hostFrom(
                "jdbc:snowflake://acme.snowflakecomputing.com").orElseThrow());
        assertTrue(SnowflakeJdbc.hostFrom("not-a-url").isEmpty());
    }

    @Test
    void reportsNothingWhenTheParameterIsMissingOrEmpty() {
        assertTrue(SnowflakeJdbc.urlParam(URL, "warehouse").isEmpty());
        assertTrue(SnowflakeJdbc.urlParam("jdbc:snowflake://acme.snowflakecomputing.com", "db").isEmpty());
        assertTrue(SnowflakeJdbc.urlParam("jdbc:snowflake://acme.snowflakecomputing.com?", "db").isEmpty());
        assertTrue(SnowflakeJdbc.urlParam("jdbc:snowflake://acme.snowflakecomputing.com?db=", "db").isEmpty());
        assertTrue(SnowflakeJdbc.urlParam(null, "db").isEmpty());
    }
}

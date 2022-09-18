use oracle::{Connection, Error};

pub struct Client {
    usr: String,
    pwd: String,
    url: String,
}

pub struct OracleClient {
    client: Client,
}

impl OracleClient {
    pub fn new<S: AsRef<str>>(usr: S, pwd: S, url: S) -> OracleClient {
        OracleClient {client: Client {usr: usr.as_ref().to_string(), pwd: pwd.as_ref().to_string(), url: url.as_ref().to_string()}}
    }

    pub fn connect(&self) -> Result<Connection, Error> {
        Connection::connect(self.client.usr.to_string(), self.client.pwd.to_string(), self.client.url.to_string())
    }
}

pub fn currval_sequence(conn: &Connection, schema: &str, sequence: String) -> Result<String, Error> {
    let currval_sql = "SELECT LPAD(last_number, 5, '0') FROM all_sequences WHERE sequence_owner = :schema AND sequence_name = :sequence";
    conn.query_row_as_named::<String>(currval_sql, &[("schema", &schema), ("sequence", &sequence)])
}

pub fn nextval_sequence(conn: &Connection, schema: &str, sequence: String) -> Result<String, Error> {
    let nextval_sql = String::from("SELECT ") + schema + "." + &sequence + ".nextval FROM DUAL";
    conn.query_row_as::<String>(&nextval_sql, &[])
}
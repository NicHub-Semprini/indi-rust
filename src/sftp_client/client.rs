use std::{io::{BufReader, BufRead, Write, BufWriter}, path::PathBuf, fs::File};

use remotefs::{RemoteFs, RemoteError};
use remotefs_ssh::{SftpFs, SshOpts};

pub static DEFAULT_PORT: u16 = 22;

pub struct Client {
    host: String,
    port: u16,
    usr: String,
    pwd: String,
}

pub struct SftpClient {
    client: Client,
}

impl SftpClient {
    pub fn new<S: AsRef<str>>(host: S, opt_port: Option<u16>, usr: S, pwd: S) -> SftpClient {
        let final_port = opt_port.unwrap_or(DEFAULT_PORT);
        SftpClient {client: Client {host: host.as_ref().to_string(), port: final_port, usr: usr.as_ref().to_string(), pwd: pwd.as_ref().to_string()}}
    }

    pub fn connect(&self) -> Result<SftpFs, RemoteError> {
        let mut client: SftpFs = SshOpts::new(self.client.host.to_string()).port(self.client.port).username(self.client.usr.to_string()).password(self.client.pwd.to_string()).into();
        match client.connect() {
            Ok(_) => Ok(client),
            Err(e) => Err(e),
        }
    }
}

pub fn sftp_connect(host: String, port: u16, usr: String, pwd: String) -> SftpFs {
    let client = SftpClient::new(&host, Some(port), &usr, &pwd);
    match client.connect() {
        Ok(s) => s,
        Err(e) => panic!("Cannot connect to {}:{} -> {:?}", host, port, e),
    }
}

pub fn sftp_get(client: &mut SftpFs, remote_file: &remotefs::File, local_path: &PathBuf, prefix: String) {
    let is = client.open(remote_file.path()).unwrap();
    let mut reader = BufReader::new(is);
    let mut buf = String::new();
    let mut res = reader.read_line(&mut buf);
    let filename = prefix + "_" + remote_file.name().as_str();
    let mut final_path = local_path.clone();
    final_path.push(filename);
    let mut file = File::create(&final_path).unwrap();
    while res.is_ok() && res.unwrap() > 0 {
        file.write_all(buf.as_bytes()).unwrap();
        buf.clear();
        res = reader.read_line(&mut buf);
    }
    println!("Downloaded file: {:?} -> {:?}", remote_file.path(), final_path);
}

pub fn sftp_put(client: &mut SftpFs, local_file: &PathBuf, remote_path: &PathBuf) {
    let file = File::open(local_file).unwrap();
    let metadata = remotefs::fs::Metadata::from(file.metadata().unwrap());
    let ws = client.append(remote_path, &metadata).unwrap();
    let mut writer = BufWriter::new(ws);
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut res = reader.read_line(&mut buf);
    while res.is_ok() && res.unwrap() > 0 {
        writer.write_all(buf.as_bytes()).unwrap();
        buf.clear();
        res = reader.read_line(&mut buf);
    }
}

pub fn sftp_rm(client: &mut SftpFs, remote_file: &remotefs::File) {
    client.remove_file(remote_file.path()).unwrap();
    println!("Deleted remote file: {:?}", remote_file.path());
}
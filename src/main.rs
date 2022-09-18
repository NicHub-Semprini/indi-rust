use std::{fs::{File, self, rename, remove_file, remove_dir_all}, io::{BufReader, BufRead, Write, BufWriter}, path::{Path, PathBuf}, time::SystemTime, env};
use oracle::{Connection, sql_type::{Timestamp, Blob, Lob}};
use chrono::{Datelike, Timelike, DateTime, Utc};
use regex::Regex;
use remotefs::RemoteFs;
use remotefs_ssh::SftpFs;

use crate::sql_client::client::OracleClient;
use crate::sftp_client::client;

mod sql_client;
mod sftp_client;

/* ENVIRONMENT INDEPENDANT CONFIGURATIONS */
static GENERAL_ROOT: &str = "../rootPath";
static GENERAL_ARCHIVE: &str = "archive";
static GENERAL_FAILURE: &str = "failure";
static GENERAL_WORKSPACE: &str = "workspace";
static GENERAL_SYSTEM: &str = "SAMPLE_SYSTEM";
static GENERAL_FLOW: &str = "SAMPLE_FLOW";
static GENERAL_BATCH_NAME: &str = "SAMPLE_BATCH_NAME";
static SOURCE_SFTP_DELETE_REMOTE: bool = true;
static SOURCE_SFTP_CHECK_LASTMTIME: bool = true;
static SOURCE_SFTP_LASTMTIME: u64 = 30;
static SOURCE_ENCODING: &str = "UTF_8";
static SOURCE_HEADER_ENABLE: bool = true;
static SOURCE_SEQUENCE_INDEX: usize = 38;
static SOURCE_FOOTER_ENABLE: bool = true;
static SOURCE_RECORDS_NUMBER_LEN: usize = 8;
static SOURCE_RECORDS_NUMBER_INDEX: usize = 43;
static OBT_SEQUENCE_SCHEMA: &str = "OBT_SCHEMA";
static LEGACY_SEQUENCE_SCHEMA: &str = "LEGACY_SCHEMA";

/* CONSTANTS */
static SOURCE: &str = "source";
static LEGACY: &str = "legacy";
static OBT: &str = "obt";
static TIMESTAMP_FORMAT: &str = "%Y%m%d%H%M%S%3f";

fn main() {
    let client_dbaindi = OracleClient::new(env::var("DB_INDI_USERNAME").unwrap(), env::var("DB_INDI_PASSWORD").unwrap(), env::var("DB_INDI_URL").unwrap());
    let conn_dbaindi = client_dbaindi.connect().unwrap();
    let client_dbaobt = OracleClient::new(env::var("DB_OBT_USERNAME").unwrap(), env::var("DB_OBT_PASSWORD").unwrap(), env::var("DB_OBT_URL").unwrap());
    let conn_dbaobt = client_dbaobt.connect().unwrap();
    loop {
        // 1. Initialize File system (paths creation and workspace cleanup)
        let workspace_legacy = init_path(vec![GENERAL_ROOT, GENERAL_SYSTEM, GENERAL_FLOW, GENERAL_WORKSPACE, LEGACY], true);
        let workspace_obt = init_path(vec![GENERAL_ROOT, GENERAL_SYSTEM, GENERAL_FLOW, GENERAL_WORKSPACE, OBT], true);
        let archive_source = init_path(vec![GENERAL_ROOT, GENERAL_SYSTEM, GENERAL_FLOW, GENERAL_ARCHIVE, SOURCE], false);
        let archive_legacy = init_path(vec![GENERAL_ROOT, GENERAL_SYSTEM, GENERAL_FLOW, GENERAL_ARCHIVE, LEGACY], false);
        let failure_source = init_path(vec![GENERAL_ROOT, GENERAL_SYSTEM, GENERAL_FLOW, GENERAL_FAILURE, SOURCE], false);
        let failure_legacy = init_path(vec![GENERAL_ROOT, GENERAL_SYSTEM, GENERAL_FLOW, GENERAL_FAILURE, LEGACY], false);
        let failure_obt = init_path(vec![GENERAL_ROOT, GENERAL_SYSTEM, GENERAL_FLOW, GENERAL_FAILURE, OBT], false);
        // 2. Download all source files from sftp server
        let mut client_sftp = client::sftp_connect(env::var("SOURCE_SFTP_HOST").unwrap(), sftp_client::client::DEFAULT_PORT, env::var("SOURCE_SFTP_USERNAME").unwrap(), env::var("SOURCE_SFTP_PASSWORD").unwrap());
        let sources = sftp_find_sources(&mut client_sftp, env::var("SOURCE_SFTP_PATH").unwrap(), env::var("SOURCE_FILE").unwrap());
        for source in sources {
            let now: DateTime<Utc> = SystemTime::now().into();
            client::sftp_get(&mut client_sftp, &source, &failure_source, now.format(TIMESTAMP_FORMAT).to_string());
            if SOURCE_SFTP_DELETE_REMOTE {
                client::sftp_rm(&mut client_sftp, &source);
            }
        }
        // 3. Select source file (oldest one)
        let sources = fs::read_dir(&failure_source).unwrap();
        let mut source_files: Vec<PathBuf> = sources.map(|f| {f.unwrap().path()}).collect();
        source_files.sort();
        println!("Source files: {:?}", source_files);
        match source_files.first() {
            Some(f) => {
                println!("Working on source file: {:?}", f);
                // 4. Split lines based on movement code
                let source_filename = f.file_name().unwrap().to_str().unwrap().to_string().split_once("_").unwrap().1.to_string();
                let source = File::open(&f).unwrap();
                let mut legacy_path = PathBuf::from(&workspace_legacy);
                legacy_path.push(LEGACY.to_string() + "_tmp");
                println!("Temp legacy file: {:?}", legacy_path);
                let legacy = File::create(&legacy_path).unwrap();
                let mut obt_path = PathBuf::from(&workspace_obt);
                obt_path.push(OBT.to_string() + "_tmp");
                println!("Temp obt file: {:?}", obt_path);
                let obt = File::create(&obt_path).unwrap();
                let tot_lines: usize = linecount::count_lines(fs::File::open(f).unwrap()).unwrap();
                println!("Source total lines: {:?}", tot_lines);
                let br = BufReader::new(source);
                let mut bw_legacy = BufWriter::new(&legacy);
                let mut bw_obt = BufWriter::new(&obt);
                let mut legacy_lines:u8 = 0;
                let mut obt_lines: u8 = 0;
                let legacy_seq = db_select_sequence(&conn_dbaindi, LEGACY_SEQUENCE_SCHEMA, GENERAL_SYSTEM, LEGACY, source_filename.clone());
                let obt_seq = db_select_sequence(&conn_dbaindi, OBT_SEQUENCE_SCHEMA, GENERAL_SYSTEM, OBT, source_filename.clone());
                for (i, line) in br.lines().enumerate(){
                    let line = line.unwrap();
                    println!("Read line {:?}: {:?}", i, line);
                    if i == 0 && SOURCE_HEADER_ENABLE { // Header
                        let legacy_header = replace(&line, SOURCE_SEQUENCE_INDEX, legacy_seq.to_owned());
                        bw_legacy.write((legacy_header + "\n").as_bytes()).unwrap();
                        let obt_header = replace(&line, SOURCE_SEQUENCE_INDEX, obt_seq.to_owned());
                        bw_obt.write((obt_header + "\n").as_bytes()).unwrap();
                    } else if i == tot_lines && SOURCE_FOOTER_ENABLE { // Footer
                        let mut legacy_lines = legacy_lines.to_string().chars().collect::<Vec<char>>();
                        while legacy_lines.len() < SOURCE_RECORDS_NUMBER_LEN {
                            legacy_lines.insert(0, '0');
                        }
                        let legacy_footer = replace(&line, SOURCE_SEQUENCE_INDEX, legacy_seq.to_owned());
                        let legacy_footer = replace(&legacy_footer, SOURCE_RECORDS_NUMBER_INDEX, legacy_lines.iter().collect());
                        bw_legacy.write((legacy_footer + "\n").as_bytes()).unwrap();
                        let mut obt_lines = obt_lines.to_string().chars().collect::<Vec<char>>();
                        while obt_lines.len() < SOURCE_RECORDS_NUMBER_LEN {
                            obt_lines.insert(0, '0');
                        }
                        let obt_footer = replace(&line, SOURCE_SEQUENCE_INDEX, obt_seq.to_owned());
                        let obt_footer = replace(&obt_footer, SOURCE_RECORDS_NUMBER_INDEX, obt_lines.iter().collect());
                        bw_obt.write((obt_footer + "\n").as_bytes()).unwrap();
                    } else { // Body
                        match get_vin(&line) {
                            Some(vin) => {
                                let exists = db_exists_vin(&conn_dbaobt, vin);
                                if exists {
                                    println!("OBT");
                                    bw_obt.write((line.to_owned() + "\n").as_bytes()).unwrap();
                                    obt_lines += 1;
                                } else {
                                    println!("legacy");
                                    bw_legacy.write((line.to_owned() + "\n").as_bytes()).unwrap();
                                    legacy_lines += 1;
                                }
                            },
                            None => {
                                println!("unknown movement -> legacy");
                                bw_legacy.write((line.to_owned() + "\n").as_bytes()).unwrap();
                                legacy_lines += 1;
                            }
                        }
                    }
                }
                bw_legacy.flush().unwrap();
                bw_obt.flush().unwrap();
                // Delete output files if empties
                let mut min_lines = 1;
                if SOURCE_HEADER_ENABLE {
                    min_lines += 1;
                }
                if SOURCE_FOOTER_ENABLE {
                    min_lines += 1;
                }
                if legacy_lines < min_lines {
                    fs::remove_file(&legacy_path).unwrap();
                    println!("Deleted empty legacy file: {:?}", legacy_path);
                } else {
                    db_nextval_sequence(&conn_dbaindi, LEGACY_SEQUENCE_SCHEMA, GENERAL_SYSTEM, LEGACY, source_filename.clone());
                }
                if obt_lines < min_lines {
                    fs::remove_file(&obt_path).unwrap();
                    println!("Deleted obt file: {:?}", obt_path);
                } else {
                    db_nextval_sequence(&conn_dbaindi, OBT_SEQUENCE_SCHEMA, GENERAL_SYSTEM, OBT, source_filename.clone());
                }
                // 5. Archive source file
                archive_file(f.to_owned(), archive_source.to_owned(), TIMESTAMP_FORMAT);
                // Place output legacy file in upload queue
                let legacies = fs::read_dir(&workspace_legacy).unwrap();
                let mut legacy_files: Vec<PathBuf> = legacies.map(|f| {f.unwrap().path()}).collect();
                legacy_files.sort();
                println!("Temp legacy files: {:?}", legacy_files);
                for l in legacy_files {
                    let mut renamed_legacy_file = l.parent().unwrap().to_owned();
                    renamed_legacy_file.push(&source_filename);
                    rename(l, &renamed_legacy_file).unwrap();
                    let failure_legacy_file = archive_file(renamed_legacy_file, failure_legacy.to_owned(), TIMESTAMP_FORMAT);
                    println!("Moved under legacy queue file: {:?}", failure_legacy_file);
                }
                // Place output obt file in insert queue
                let obts = fs::read_dir(&workspace_obt).unwrap();
                let mut obt_files: Vec<PathBuf> = obts.map(|f| {f.unwrap().path()}).collect();
                obt_files.sort();
                println!("Temp obt files: {:?}", obt_files);
                for o in obt_files {
                    let mut renamed_obt_file = o.parent().unwrap().to_owned();
                    renamed_obt_file.push(&source_filename);
                    rename(o, &renamed_obt_file).unwrap();
                    let failure_obt_file = archive_file(renamed_obt_file, failure_obt.to_owned(), TIMESTAMP_FORMAT);
                    println!("Moved under obt queue file: {:?}", failure_obt_file);
                }
            },
            None => {
                println!("There are no available sources");
                break;
            }
        }
        // 6. Upload output files
        // Upload legacy files on sftp server
        let legacies = fs::read_dir(&failure_legacy).unwrap();
        let mut legacy_files: Vec<PathBuf> = legacies.map(|f| {f.unwrap().path()}).collect();
        legacy_files.sort();
        println!("Final legacy files: {:?}", legacy_files);
        if !legacy_files.is_empty() {
            let mut client_sftp = client::sftp_connect(env::var("LEGACY_SFTP_HOST").unwrap(), sftp_client::client::DEFAULT_PORT, env::var("LEGACY_SFTP_USERNAME").unwrap(), env::var("LEGACY_SFTP_PASSWORD").unwrap());
            for f in legacy_files {
                let mut remote_path = PathBuf::from(env::var("LEGACY_SFTP_PATH").unwrap());
                let filename = f.file_name().unwrap().to_str().unwrap().split_once("_").unwrap().1.to_string();
                remote_path.push(filename);
                client::sftp_put(&mut client_sftp, &f, &remote_path);
                println!("Uploaded legacy file: {:?} -> {:?}", f, remote_path);
                archive_file(f, archive_legacy.to_owned(), TIMESTAMP_FORMAT);
            }
        }
        // Insert obt files into database
        let obts = fs::read_dir(&failure_obt).unwrap();
        let mut obt_files: Vec<PathBuf> = obts.map(|f| {f.unwrap().path()}).collect();
        obt_files.sort();
        println!("Final obt files: {:?}", obt_files);
        if !obt_files.is_empty() {
            for f in obt_files {
                db_insert(&conn_dbaobt, &f);
                conn_dbaobt.commit().unwrap();
                remove_file(f).unwrap();
            }
        }
    }
}

fn init_path(dirs: Vec<&str>, clean: bool) -> PathBuf {
    let path: PathBuf = dirs.iter().collect();
    if clean && path.exists() && path.is_dir() {
        remove_dir_all(&path).unwrap();
    }
    match fs::create_dir_all(&path) {
        Ok(()) => {
            println!("Initialized path: {:?} (clean: {:?})", path, clean);
            path
        },
        Err(e) => panic!("Cannot initialize path {:?}! {}", path, e),
    }
}

fn sftp_find_sources(client: &mut SftpFs, path: String, filename: String) -> Vec<remotefs::File> {
    let entries = client.list_dir(Path::new(&path)).unwrap();
    let mut sources: Vec<remotefs::File> = Vec::new();
    let re = Regex::new(&filename).unwrap();
    let now = SystemTime::now();
    for entry in entries {
        if entry.is_file()
            && (!SOURCE_SFTP_CHECK_LASTMTIME || (now.duration_since(entry.metadata.modified.unwrap_or(SystemTime::UNIX_EPOCH)).unwrap().as_secs() >= SOURCE_SFTP_LASTMTIME))
            && re.is_match(entry.name().as_str()) {
                println!("Localized remote source file: {:?}", entry);
                sources.push(entry);
        }
    }
    sources
}

fn archive_file(file: PathBuf, new_path: PathBuf, format: &str) -> PathBuf {
    let now: DateTime<Utc> = SystemTime::now().into();
    let filename = now.format(format).to_string() + "_" + file.file_name().unwrap().to_str().unwrap();
    let mut final_path = new_path.clone();
    final_path.push(filename);
    rename(&file, &final_path).unwrap();
    println!("Archived file: {:?} -> {:?}", file, final_path);
    final_path
}

fn replace(string: &String, start: usize, replacement: String) -> String {
    let mut new_string = String::from(string);
    let end = replacement.len() + start;
    new_string.replace_range(start..end, &replacement);
    new_string
    
}

fn db_exists_vin(conn: &Connection, vin: &str) -> bool {
    let exists_sql = "SELECT COUNT(*) FROM OBT_VEHICLES WHERE VIN = :vin";
    conn.query_row_as_named::<i32>(exists_sql, &[("vin", &vin)]).unwrap() != 0
}

fn db_select_sequence(conn: &Connection, schema: &str, system: &str, destination_type: &str, filename: String) -> String {
    let sequence = String::from("INDI") + "_" + system + "_" + filename.chars().rev().collect::<String>().split_once("_").unwrap().1.chars().rev().collect::<String>().as_str() + "_" + destination_type.to_uppercase().as_str() + "_SEQ";
    sql_client::client::currval_sequence(conn, schema, sequence).unwrap()
}

fn db_nextval_sequence(conn: &Connection, schema: &str, system: &str, destination_type: &str, filename: String) {
    let sequence = String::from("INDI") + "_" + system + "_" + filename.chars().rev().collect::<String>().split_once("_").unwrap().1.chars().rev().collect::<String>().as_str() + "_" + destination_type.to_uppercase().as_str() + "_SEQ";
    sql_client::client::nextval_sequence(conn, schema, sequence).unwrap();
}

fn db_insert(conn: &Connection, path: &PathBuf) {
    let file = fs::File::open(path).unwrap();
    let metadata = file.metadata().unwrap();
    let name = path.file_name().unwrap().to_str().unwrap().split_once("_").unwrap().1.to_string();
    let length = metadata.len();
    let tmp: DateTime<Utc> = metadata.created().unwrap_or(metadata.created().unwrap_or(SystemTime::now())).into();
    let creation = Timestamp::new(tmp.year(), tmp.month(), tmp.day(), tmp.hour(), tmp.minute(), tmp.second(), tmp.nanosecond());
    let tmp: DateTime<Utc> = metadata.modified().unwrap_or(SystemTime::now()).into();
    let update = Timestamp::new(tmp.year(), tmp.month(), tmp.day(), tmp.hour(), tmp.minute(), tmp.second(), tmp.nanosecond());
    let lines: usize = linecount::count_lines(fs::File::open(path).unwrap()).unwrap();
    let nextval_sql = "SELECT OBT_FILE_BLOB_SEQ.NEXTVAL FROM DUAL";
    let id = conn.query_row_as::<i64>(nextval_sql, &[]).unwrap();
    println!("Got BLOB_SEQ next val: {:?}", id);
    let insert_sql = "INSERT INTO OBT_FILE_BLOB (ID, FILE_NAME, FILE_LENGTH, FILE_CREATION, FILE_UPDATE, FILE_ENCODING, FLOW_NAME, FILE_TOTAL_ROWS) VALUES (:id, :name, :length, :creation, :updation, :encoding, :flow, :file_total_rows)";
    let mut stmt = conn.statement(insert_sql).build().unwrap();
    stmt.execute_named(&[("id", &id), ("name", &name), ("length", &length), ("creation", &creation), ("updation", &update), ("encoding", &SOURCE_ENCODING), ("flow", &GENERAL_BATCH_NAME), ("file_total_rows", &lines)]).unwrap();
    println!("Inserted BLOB record");
    db_insert_blob(conn, id, &file).unwrap();
    let update_sql = "UPDATE OBT_FILE_BLOB SET STATUS = 100 WHERE ID = :id";
    let mut stmt = conn.statement(update_sql).build().unwrap();
    stmt.execute_named(&[("id", &id)]).unwrap();
    println!("Updated status");
}

fn db_insert_blob(conn: &Connection, id: i64, file: &fs::File) -> Result<(), std::io::Error> {
    let sql = "SELECT FILE_BLOB FROM OBT_FILE_BLOB WHERE ID = :id";
    let mut statement_builder = conn.statement(sql);
    match statement_builder.lob_locator().build() {
        Ok(mut stmt) => {
            match stmt.query_row_as_named::<Blob>(&[("id", &id)]) {
                Ok(mut blob) => {
                    match blob.open_resource() {
                        Ok(()) => {
                            let mut reader = BufReader::new(file);
                            let mut buf = String::new();
                            let mut res = reader.read_line(&mut buf);
                            while res.is_ok() && res.unwrap() > 0 {
                                blob.write_all(buf.as_bytes())?;
                                buf.clear();
                                res = reader.read_line(&mut buf);
                            }
                            match blob.close_resource() {
                                Ok(()) => println!("Inserted BLOB bytes"),
                                Err(e) => println!("Errore sull'open del BLOB -> {}", e),
                            }
                        },
                        Err(e) => println!("Errore sull'open del BLOB -> {}", e),
                    }
                },
                Err(e) => println!("Errore sulla query del BLOB -> {}", e),
            }
        },
        Err(e) => println!("Errore sul locator del BLOB -> {}", e),
    }
    Ok(())
}

fn get_vin<'a>(line: &'a String) -> Option<&'a str> {
    let vin = &line[0..4];
    match vin {
        "6560" | "6564" => Some(&line[29..46]),
        _ => None,
    }
}

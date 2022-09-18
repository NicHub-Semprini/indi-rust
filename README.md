# indi-rust
Rust version of an integration batch program previously implemented with Spring Batch framework.

## Work loop
1. Initialize file system
2. Download all source files from sftp server
3. Select source file (oldest one)
4. Split lines based on movement code
5. Archive source file
6. Upload output files
    * Upload legacy files on sftp server
    * Insert obt files into database
#![crate_id = "rustyredis#0.1"]
#![desc = "A Rust client library for Redis based off hiredis"]
#![crate_type = "lib"]

use std::io::net::tcp::TcpStream;
use std::io::BufReader;
use std::int::parse_bytes;

enum Global {
  RedisReaderMaxBuf = 1024 * 16
}

enum RedisReplyType {
  RedisReplyString,
  RedisReplyInteger,
  RedisReplyArray,
  RedisReplyNil,
  RedisReplyStatus,
  RedisReplyError
}

#[deriving(Clone)]
pub enum RedisObject {
  RedisString(String),
  RedisInteger(int),
  RedisArray(Vec<RedisObject>),
  RedisNil,
  RedisStatus(String),
  RedisError(String)
}

enum RedisError {
  RedisNoError,
  RedisIoError,
  RedisProcessError
}

struct RedisReadTask {
  kind: RedisReplyType,
  elm: int,
  idx: int,
  obj: RedisObject
}

struct RedisReader {
  err: RedisError,
  errstr: String,
  buf: String,
  reply: RedisObject,
  pos: uint,
  len: uint,
  rstack: [RedisReadTask, ..9],
  ridx: int
}

pub struct RedisContext {
  err: RedisError,
  errstr: String,
  tcpstream: TcpStream,
  flags: int, 
  obuf: String,
  reader: RedisReader
}

impl RedisReadTask {
  fn new() -> RedisReadTask {
    RedisReadTask {
      kind: RedisReplyNil,
      elm: -1,
      idx: -1,
      obj: RedisNil,
    }
  }
}

impl RedisReader {
  fn create_integer(&self, s: &str) -> Result<RedisObject, &str> {
    match parse_bytes(s.as_bytes(), 10) {
      Some(n) => Ok(RedisInteger(n)),
      None => Err("Invalid integer for create_integer")
    }
  }

  fn process_line_item(&mut self) -> Result<(), &str> {
    let mut reader = BufReader::new(self.buf.as_bytes());

    match reader.read_line() {
      Ok(s) => {
        let mut ns = s.clone();
        ns.shift_char(); // this is dirty and quick
        let nns = ns.as_slice().trim();

        match self.rstack[self.ridx as uint].kind {
          RedisReplyStatus => self.reply = RedisStatus(nns.to_str()),
          RedisReplyError => self.reply = RedisError(nns.to_str()),
          RedisReplyInteger => match self.create_integer(nns) {
            Ok(n) => self.reply = n,
            Err(e) => return Err(e)
          },
          _ => return Err("Invalid type for process_line_item")
        };
        Ok(())
      },
      Err(e) => Err(e.desc)
    }
  }

  fn process_bulk_item(&mut self) -> Result<(), &str> {
    unimplemented!();
  }
  
  fn process_multi_bulk_item(&mut self) -> Result<(), &str> {
    unimplemented!();
  }

  fn process_item(&mut self) -> Result<(), &str> {
    match self.rstack[self.ridx as uint].kind {
      RedisReplyError => self.process_line_item(),
      RedisReplyStatus => self.process_line_item(),
      RedisReplyInteger => self.process_line_item(),
      RedisReplyString => self.process_bulk_item(), 
      RedisReplyArray => self.process_multi_bulk_item(),
      _ => Err("This should never happen")
    }
  }

  fn check_reply_type(&mut self) -> Result<(), &str> {
    let mut kind: RedisReplyType;
    let slice = self.buf.as_slice();
    match slice.char_at(0) {
      '-' => kind = RedisReplyError,
      '+' => kind = RedisReplyStatus,
      ':' => kind = RedisReplyInteger,
      '$' => kind = RedisReplyString,
      '*' => kind = RedisReplyArray,
      _ =>  return Err("Failed reading control byte")
    }

    self.rstack[self.ridx as uint].kind = kind;
    Ok(())
  }

  fn process_reply(&mut self) -> Result<Option<RedisObject>, &str> {
    if self.len == 0 {
      return Ok(None) 
    }

    if self.ridx == -1 {
      self.ridx = 0;
    }

    while self.ridx >= 0 {
      match self.check_reply_type(){
        Err(s) => { self.set_error(RedisProcessError, s); return Err(s) },
        _ => match self.process_item() {
               Err(s) => { self.set_error(RedisProcessError, s); return Err(s) },
               _ => self.ridx = self.ridx - 1
             }
      }
    }

    Ok(Some(self.reply.clone()))
  }

  fn feed(&mut self, buf: &mut [u8], len: uint){
    let mut i = 0;
    while i < len {
      let c = buf[i].to_ascii();
      self.buf.push_char(c.to_char());
      i = i + 1;
    }
    self.len = len;
  }

  fn set_error(&mut self, err: RedisError, errstr: &str){
    self.err = err;
    self.errstr = errstr.to_str();
  }

  fn new() -> RedisReader {
    RedisReader {
      err: RedisNoError,
      errstr: String::new(),
      buf: String::new(),
      reply: RedisNil,
      pos: 0,
      len: 0,
      rstack: [RedisReadTask::new(),
               RedisReadTask::new(),
               RedisReadTask::new(),
               RedisReadTask::new(),
               RedisReadTask::new(),
               RedisReadTask::new(),
               RedisReadTask::new(),
               RedisReadTask::new(),
               RedisReadTask::new()],
      ridx: -1
    }
  }
}

impl RedisContext {
  fn format_command(&self, format: &str) -> String {
    let mut argc: uint = 0;
    let mut c = String::new();
    let mut finalc = String::new();

    for arg in format.split(' '){
      let arglen = arg.len();
      c = c.append("$");
      c = c.append(arglen.to_str().as_slice());
      c = c.append("\r\n");
      c = c.append(arg);
      c = c.append("\r\n");
      argc = argc + 1;
    }
    finalc = finalc.append("*");
    finalc = finalc.append(argc.to_str().as_slice());
    finalc = finalc.append("\r\n");
    finalc = finalc.append(c.as_slice());
    finalc
  }

  fn set_error(&mut self, err: RedisError, errstr: &str){
    self.err = err;
    self.errstr = errstr.to_str();
  }

  fn get_reply_reader(&mut self) -> Result<Option<RedisObject>, &str> {
    self.reader.process_reply()
  }

  fn buffer_write(&mut self) -> int {
    match self.tcpstream.write_str(self.obuf.as_slice()){
      Err(e) => { self.set_error(RedisIoError, e.desc ); 1 }
      _ => 0
    }
  }

  fn buffer_read(&mut self) -> int {
    let mut buf = [0, ..RedisReaderMaxBuf as uint];
    let res = self.tcpstream.read(buf);
    match res {
      Ok(s) => { self.reader.feed(buf, s); 0 },
      Err(e) => { self.set_error(RedisIoError, e.desc); 1 }
    }

  }

  fn block_for_reply(&mut self) -> Result<Option<RedisObject>, &str> {
    let o = try!(self.get_reply_reader());
    match o {
      Some(s) => Ok(Some(s)),
      None => {
        self.buffer_write();
        self.buffer_read();
        let o2 = try!(self.get_reply_reader());
        match o2{
          Some(s) => Ok(Some(s)),
          None => Err("No reply was found")
        }
      }
    }
  }

  pub fn connect(ip: &str, port: u16) -> RedisContext {
    RedisContext {
      err: RedisNoError,
      errstr: String::new(),
      tcpstream: match TcpStream::connect(ip, port) {
        Ok(s) => s,
        Err(e) => fail!(e.desc)
      },
      flags: 0,
      obuf: String::new(),
      reader: RedisReader::new()
    }
  }

  pub fn command(&mut self, command: &str) -> Result<Option<RedisObject>, &str> {
    self.obuf = self.format_command(command);
    self.block_for_reply()
  }
}


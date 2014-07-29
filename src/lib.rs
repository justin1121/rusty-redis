#![crate_name = "rusty-redis"]
#![desc = "A Rust client library for Redis based off hiredis"]
#![crate_type = "lib"]

// TODO: expand tests
// TODO: multi bulk items
// TODO: documentation
// TODO: async
// TODO: high level commands?

use std::io::net::tcp::TcpStream;

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
  RedisInteger(i64),
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
  buf: Vec<u8>,
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
  flags: int, // async flag?
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
  fn create_integer(&self, s: &str) -> Result<RedisObject, &'static str> {
    match from_str::<i64>(s) {
      Some(n) => Ok(RedisInteger(n)),
      None => Err("Error: Invalid integer for create_integer")
    }
  }

  fn create_string(&mut self, len: uint) -> Result<RedisObject, &'static str> {
    match self.read_line() {
      Ok(s) => {
        println!("{}, {}, {}", len, s.len(), s);
        if len != s.len() {
          return Err("Error: Invalid string for create_string: lengths are not equal");
        }
        Ok(RedisString(s.as_slice()
                        .trim()
                        .to_string()))
      },
      Err(e) => Err(e)
    }
  }

  fn find_newline(&self) -> Option<uint> {
    for i in range(self.pos, self.buf.len()) {
      if self.buf[i] == '\r' as u8 &&
         self.buf[i + 1] == '\n' as u8 {
        return Some(i as uint);
      }
    }
    None
  }

  fn read_line(&mut self) -> Result<String, &'static str> {
    let pos = match self.find_newline() {
      Some(u) => u,
      None => return Err("Error: Could not find newline")
    };

    let mut line = String::new();
    for i in range(self.pos, pos) {
      line.push_char(self.buf[i] as char);
    }
    self.pos = pos + 2;
    Ok(line)
  }

  fn process_line_item(&mut self) -> Result<(), &'static str> {
    match self.read_line() {
      Ok(s) => {
        let ns = s.clone();
        let nns = ns.as_slice().trim();

        self.reply = match self.rstack[self.ridx as uint].kind {
          RedisReplyStatus => RedisStatus(nns.to_string()),
          RedisReplyError => RedisError(nns.to_string()),
          RedisReplyInteger => match self.create_integer(nns) {
            Ok(n) => n,
            Err(e) => return Err(e)
          },
          _ => return Err("Error: Invalid type for process_line_item")
        };
        Ok(())
      },
      Err(e) => Err(e)
    }
  }

  fn process_bulk_item(&mut self) -> Result<(), &'static str> {
    match self.read_line() {
      Ok(s) => {
        let ns = s.clone();
        let nns = ns.as_slice().trim();

        let mut len: uint = 0;
        match from_str::<i64>(nns){
          Some(n) => {
            if n == -1{
              self.rstack[self.ridx as uint].kind = RedisReplyNil;
            }
            else{
              len = n as uint;
            }
          },
          None => { return Err("Error: Invalid length in reply for process_bulk_item") }
        };

        self.reply = match self.rstack[self.ridx as uint].kind {
          RedisReplyString => match self.create_string(len) {
            Ok(s) => s,
            Err(e) => return Err(e)
          },
          RedisReplyNil => RedisNil,
          _ => return Err("Error: Invalid type for process_bulk_item")
        };
        Ok(())
      }
      Err(e) => Err(e)
    }
  }

  fn process_multi_bulk_item(&mut self) -> Result<(), &'static str> {
    unimplemented!();
  }

  fn process_item(&mut self) -> Result<(), &'static str> {
    match self.rstack[self.ridx as uint].kind {
      RedisReplyError => self.process_line_item(),
      RedisReplyStatus => self.process_line_item(),
      RedisReplyInteger => self.process_line_item(),
      RedisReplyString => self.process_bulk_item(),
      RedisReplyArray => self.process_multi_bulk_item(),
      _ => Err("Error: This should never happen")
    }
  }

  fn check_reply_type(&mut self) -> Result<(), &'static str> {
    let mut kind: RedisReplyType;
    kind = match self.buf[self.pos] as char {
      '-' => RedisReplyError,
      '+' => RedisReplyStatus,
      ':' => RedisReplyInteger,
      '$' => RedisReplyString,
      '*' => RedisReplyArray,
      _ =>  return Err("Error: Failed reading control byte")
    };
    self.pos += 1;

    self.rstack[self.ridx as uint].kind = kind;
    Ok(())
  }

  fn process_reply(&mut self) -> Result<Option<RedisObject>, &'static str> {
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
    self.buf.truncate(0);
    self.len = 0;
    self.pos = 0;
    Ok(Some(self.reply.clone()))
  }

  fn feed(&mut self, buf: &[u8], len: uint){
    self.buf.push_all(buf);
    self.len = len;
  }

  fn set_error(&mut self, err: RedisError, errstr: &'static str){
    self.err = err;
    self.errstr = errstr.to_string();
  }

  fn new() -> RedisReader {
    RedisReader {
      err: RedisNoError,
      errstr: String::new(),
      buf: Vec::new(), // with_capacity??
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
  fn format_command(&self, cmd: &str, args: &[&str]) -> String {
    let mut argc: uint = 0;
    let mut c = String::new();
    let mut finalc = String::new();

    let mut len = cmd.len();
    c = c.append("$");
    c = c.append(len.to_string().as_slice());
    c = c.append("\r\n");
    c = c.append(cmd);
    c = c.append("\r\n");
    argc += 1;

    for arg in args.iter(){
      len = arg.len();
      c = c.append("$");
      c = c.append(len.to_string().as_slice());
      c = c.append("\r\n");
      c = c.append(*arg);
      c = c.append("\r\n");
      argc += 1;
    }
    finalc = finalc.append("*");
    finalc = finalc.append(argc.to_string().as_slice());
    finalc = finalc.append("\r\n");
    finalc = finalc.append(c.as_slice());
    finalc
  }

  fn set_error(&mut self, err: RedisError, errstr: &'static str){
    self.err = err;
    self.errstr = errstr.to_string();
  }

  fn check_error(&self) -> bool {
    match self.err {
      RedisNoError => false,
      _ => true
    }
  }

  fn get_reply_reader(&mut self) -> Result<Option<RedisObject>, &'static str> {
    self.reader.process_reply()
  }

  fn buffer_write(&mut self) {
    match self.tcpstream.write_str(self.obuf.as_slice()){
      Err(e) => { self.set_error(RedisIoError, e.desc ); }
      _ => ()
    }
  }

  fn buffer_read(&mut self) {
    let mut buf = [0, ..RedisReaderMaxBuf as uint];
    let res = self.tcpstream.read(buf);
    match res {
      Ok(s) => { self.reader.feed(buf, s); },
      Err(e) => { self.set_error(RedisIoError, e.desc); }
    }

  }

  fn block_for_reply(&mut self) -> Result<Option<RedisObject>, &'static str> {
    let o = try!(self.get_reply_reader());
    match o {
      Some(s) => Ok(Some(s)),
      None => {
        self.buffer_write();
        self.buffer_read();

        if self.check_error() {
          return Err("Error. Check error members");
        }
        let o2 = try!(self.get_reply_reader());
        match o2{
          Some(s) => Ok(Some(s)),
          None => Err("Error: No reply was found. Check error members")
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

  pub fn command(&mut self, cmd: &str, args: &[&str]) -> Result<Option<RedisObject>, &'static str> {
    self.obuf = self.format_command(cmd, args);
    self.block_for_reply()
  }
}

#[cfg(test)]
mod test {
  #[test]
  fn test_connect(){
    let _ = ::RedisContext::connect("127.0.0.1", 6379);
  }

  #[test]
  fn test_command(){
    let mut c = ::RedisContext::connect("127.0.0.1", 6379);
    let r = c.command("PING", []);

    match r {
      Ok(s) => match s {
        Some(s) => match s {
          ::RedisStatus(s) => assert!(s.eq(&"PONG".to_string())),
          _ => fail!("Didn't return correct reply type.")
        },
        None => fail!("Didn't return anything.")
      },
      Err(e) => fail!("{}", e)
    }
  }

  #[test]
  fn test_reply_types() {
    // Integer
    let mut c = ::RedisContext::connect("127.0.0.1", 6379);
    let r = c.command("APPEND", ["mykey", "te\nst"]);

    match r {
      Ok(i) => match i {
        Some(i) => match i {
          ::RedisInteger(i) => {
            assert!(i == 5);
          }
          _ => fail!("INTEGER: Didn't return correct reply type.")
        },
        None => fail!("INTEGER: Didn't return anything.")
      },
      Err(e) => fail!("INTEGER: {}", e)
    }

    // String
    let r = c.command("GET", ["mykey"]);

    match r {
      Ok(s) => match s {
        Some(s) => match s {
          ::RedisString(s) => assert!(s.eq(&"te\nst".to_string())),
          _ => fail!("STRING: Didn't return correct reply type.")
        },
        None => fail!("STRING: Didn't return anything.")
      },
      Err(e) => fail!("{}", e)
    }

    // cleanup
    let r = c.command("DEL", ["mykey"]);

    match r {
      Ok(i) => match i {
        Some(i) => match i {
          ::RedisInteger(i) => assert!(i == 1),
          _ => fail!("DELETE: cleanup failed")
        },
        None => fail!("DELETE: cleanup failed")
      },
      Err(_) => fail!("DELETE: cleanup failed")
    }
  }

  #[test]
  fn test_nil_reply() {
    let mut context = ::RedisContext::connect("127.0.0.1", 6379);
    let r = context.command("GET", ["notmykey"]);

    match r {
      Ok(s) => match s {
        Some(s) => match s {
          super::RedisNil => return,
          _ => fail!("NIL: Didn't return correct reply type.")
        },
        None => fail!("NIL: Didn't return anything."),
      },
      Err(e) => fail!("{}", e)
    }
  }
}


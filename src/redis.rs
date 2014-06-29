#![crate_id = "rustyredis#0.1"]
#![desc = "A Rust client library for Redis based off hiredis"]
#![crate_type = "lib"]

use std::io::net::tcp::TcpStream;

enum Global {
  RedisReaderMaxBuf = 1024 * 16
}

struct RedisReadTask {
  kind: int,
  elm: int,
  idx: int,
  obj: String, // this prob isn't right
}

struct RedisReader {
  err: int,
  errstr: String,
  buf: String,
  reply: String,
  pos: int,
  len: int,
  maxbuf: Global,
  
  rstack: [RedisReadTask, ..9],
  ridx: int
}

pub struct RedisContext {
  err: int,
  errstr: String,
  tcpstream: TcpStream,
  flags: int, 
  obuf: String,
  reader: RedisReader
}

impl RedisReadTask {
  fn new() -> RedisReadTask {
    RedisReadTask {
      kind: -1,
      elm: -1,
      idx: -1,
      obj: String::new(),
    }
  }
}

impl RedisReader {

  fn process_item(&self) ->Result<(), String> {
    unimplemented!();
  }

  fn process_reply(&mut self) -> Result<(), String> {
    if self.len == 0 {
      return Ok(()) 
    }

    if self.ridx == -1 {
      self.ridx = 0;
    }

    while self.ridx >= 0 {
      try!(self.process_item()); 
    }
    
    Ok(())
  }

  //fn get_reply(&mut self) -> Option<String> {
  //  Some(self.reply)
  //}

  fn feed(&mut self, buf: &mut [u8], len: uint){
    let mut i = 0;
    while i < len {
      let c = buf[i].to_ascii();
      self.buf.push_char(c.to_char());
      i = i + 1;
    }
    println!("{}", self.buf);
  }

  fn new() -> RedisReader {
    RedisReader {
      err: 0,
      errstr: String::new(),
      buf: String::new(),
      reply: String::new(),
      pos: 0,
      len: 0,
      maxbuf: RedisReaderMaxBuf,
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

  fn set_error(&mut self, err: int, errstr: &str){
    self.err = err;
    self.errstr = errstr.to_str();
  }

  fn get_reply_reader(&mut self) -> Result<Option<String>, String> {
    //self.reader.get_reply();
    Err("ERR".to_str())
  }

  fn buffer_write(&mut self) -> int {
    println!("{}", self.obuf);
    match self.tcpstream.write_str(self.obuf.as_slice()){
      Err(e) => { self.set_error(1, e.desc ); 1 }
      _ => 0
    }
  }

  fn buffer_read(&mut self) -> int {
    let mut buf = [0, ..RedisReaderMaxBuf as uint];
    let res = self.tcpstream.read(buf);
    match res {
      Ok(s) => { self.reader.feed(buf, s); 0 },
      Err(e) => { self.set_error(1, e.desc); 1 }
    }

  }

  fn block_for_reply(&mut self) -> Result<Option<String>, String> {
    let o = try!(self.get_reply_reader());
    match o {
      Some(s) => Ok(Some(s)),
      None => {
        self.buffer_write();
        self.buffer_read();
        let o2 = try!(self.get_reply_reader());
        match o2{
          Some(s) => Ok(Some(s)),
          None => Err("Error".to_str())
        }
      }
    }
  }

  pub fn connect(ip: &str, port: u16) -> RedisContext {
    RedisContext {
      err: 0,
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

  pub fn command(&mut self, command: &str) -> Result<Option<String>, String> {
    self.obuf = self.format_command(command);
    let r = try!(self.block_for_reply());
    
    match r {
      Some(s) => { println!("{}", s); Ok(Some(s)) }
      None => { println!("Error!"); Err("Error".to_str()) }
    }
  }
}


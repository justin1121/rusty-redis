fn cleanup(mut c: ::RedisContext){
  let r = c.command("DEL", ["mykey"]);

  match r {
    Ok(i) => match i {
      Some(i) => match i {
        ::RedisInteger(_) => (),
        _ => fail!("Failed to clean up")
      },
      None => fail!("Failed to clean up")
    },
    Err(_) => fail!("Failed to cleanup")
  }

  c.close();
}

fn check_append(r: Result<Option<::RedisObject>, &'static str>){
  match r {
    Ok(i) => match i {
      Some(i) => match i {
        ::RedisInteger(i) => assert!(i == 5),
        _ => fail!("INTEGER: Didn't return correct reply type.")
      },
      None => fail!("INTEGER: Didn't return anything.")
    },
    Err(e) => fail!("INTEGER: {}", e)
  }
}

#[test]
fn test_connect(){
  let c = ::RedisContext::connect("127.0.0.1", 6379);
  c.close();
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
  c.close();
}

#[test]
fn test_line_item_reply() {
  let mut c = ::RedisContext::connect("127.0.0.1", 6379);
  let r = c.command("APPEND", ["mykey", "te\nst"]);

  check_append(r);
  cleanup(c);
}

#[test]
fn test_bulk_item_reply() {
  let mut c = ::RedisContext::connect("127.0.0.1", 6379);
  let r = c.command("APPEND", ["mykey", "te\nst"]);

  check_append(r);

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

  cleanup(c);
}

#[test]
fn test_nil_reply() {
  let mut c = ::RedisContext::connect("127.0.0.1", 6379);
  let r = c.command("GET", ["notmykey"]);

  match r {
    Ok(s) => match s {
      Some(s) => match s {
        ::RedisNil => (),
        _ => fail!("NIL: Didn't return correct reply type.")
      },
      None => fail!("NIL: Didn't return anything."),
    },
    Err(e) => fail!("{}", e)
  }
  c.close();
}

pub const VERSION: u64 = 1;
pub const DEST_NAME: &str = "rrc.hub";

pub const K_V: i128 = 0;
pub const K_T: i128 = 1;
pub const K_ID: i128 = 2;
pub const K_TS: i128 = 3;
pub const K_SRC: i128 = 4;
pub const K_ROOM: i128 = 5;
pub const K_BODY: i128 = 6;
pub const K_NICK: i128 = 7;
pub const K_DST: i128 = 8;

pub const T_HELLO: u64 = 1;
pub const T_WELCOME: u64 = 2;
pub const T_JOIN: u64 = 10;
pub const T_JOINED: u64 = 11;
pub const T_PART: u64 = 12;
pub const T_PARTED: u64 = 13;
pub const T_MSG: u64 = 20;
pub const T_NOTICE: u64 = 21;
pub const T_ACTION: u64 = 22;
pub const T_PING: u64 = 30;
pub const T_PONG: u64 = 31;
pub const T_ERROR: u64 = 40;
pub const T_RESOURCE_ENVELOPE: u64 = 50;

pub const B_HELLO_CAPS: i128 = 2;
pub const B_HELLO_NICK_LEGACY: i128 = 64;
pub const B_WELCOME_HUB: i128 = 0;
pub const B_WELCOME_VER: i128 = 1;
pub const B_WELCOME_CAPS: i128 = 2;
pub const B_WELCOME_LIMITS: i128 = 3;
pub const CAP_RESOURCE_ENVELOPE: i128 = 0;
pub const CAP_ACTION: i128 = 1;
pub const CAP_DIRECT_NOTICE: i128 = 2;

pub const B_RES_ID: i128 = 0;
pub const B_RES_KIND: i128 = 1;
pub const B_RES_SIZE: i128 = 2;
pub const B_RES_SHA256: i128 = 3;
pub const B_RES_ENCODING: i128 = 4;

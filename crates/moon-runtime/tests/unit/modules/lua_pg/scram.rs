use super::*;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[test]
fn hmac_sha256_rfc4231_case2() {
    let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
    assert_eq!(
        hex(&mac),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

#[test]
fn pbkdf2_hmac_sha256_known_vectors() {
    let dk = pbkdf2_hmac_sha256_one_block(b"password", b"salt", 1);
    assert_eq!(
        hex(&dk),
        "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
    );
    let dk = pbkdf2_hmac_sha256_one_block(b"password", b"salt", 4096);
    assert_eq!(
        hex(&dk),
        "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"
    );
}

#[test]
fn scram_rfc7677_exchange() {
    let mut c = ScramSha256Client::new("user".to_string(), "pencil".to_string());
    c.client_nonce = "rOprNGfwEbeRWgbNEkqO".to_string();
    c.client_first_message_bare = format!("n={},r={}", c.username, c.client_nonce);
    c.state = State::FirstSent;

    let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                                s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    c.process_server_first(server_first).unwrap();

    let final_msg = c.prepare_final_message().unwrap();
    assert_eq!(
        final_msg,
        "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                 p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    );

    let server_final = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
    c.process_server_final(server_final).unwrap();
    assert!(c.is_authenticated());
}

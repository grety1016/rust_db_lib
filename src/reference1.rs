pub fn fn_reference1() {
    let user1 = User {
        name: String::from("Grety"),
        age: 20,
        email: String::from("grety@example.com"),
    };
    let user2 = User {
        name: String::from("Grety"),
        ..user1.clone()
    };
    println!("{:#?}", user2);
    println!("{:#?}", user1);
}
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct User {
    name: String,
    age: u8,
    email: String,
}

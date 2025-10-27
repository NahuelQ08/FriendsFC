def check_credentials(username, password):
    USERS = {
        "admin": "1234",
        "nahuel": "deporte2025"
    }
    return username in USERS and USERS[username] == password
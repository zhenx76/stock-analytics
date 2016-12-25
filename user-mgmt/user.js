var when = require('when');
var logger = require('../utility').logger;
var bcrypt = require('bcrypt');
var userStore = require('./user-store').userStore;

var User = exports.User = function(username) {
    validateProperty('username', username);

    this.username = username;
    this.id = -1;
    this.firstname = '';
    this.lastname = '';
    this.email = '';
    this.password = '';
    this.watch_list = [];
};

//////
// Class methods

User.init = function() {
    logger.info('Initializing user store');
    return userStore.loadUsers();
};

User.find = function(username) {
    return userStore.findUser(username);
};

User.create = function(jsonObj) {
    var user = new User(jsonObj.username);

    user.id = jsonObj.id;
    user.firstname = jsonObj.firstname;
    user.lastname = jsonObj.lastname;
    user.email = jsonObj.email;
    user.password = jsonObj.password;

    for (var i = 0; i < jsonObj.watch_list.length; i++) {
        user.watch_list.push(jsonObj.watch_list[i]);
    }

    return user;
};

userStore.setFactory(User.create);

//////
// Instance methods

User.prototype.clone = function() {
    var user = new User(this.username);

    user.id = this.id;
    user.firstname = this.firstname;
    user.lastname = this.lastname;
    user.email = this.email;
    user.password = this.password;

    for (var i = 0; i < this.watch_list.length; i++) {
        user.watch_list.push(this.watch_list[i]);
    }

    return user;
};

User.prototype.set = function(property, value) {
    validateProperty(property, value);
    this[property] = value;
};

User.prototype.addToWatchList = function(symbol) {
    for (var i = 0; i < this.watch_list.length; i++) {
        if (symbol == this.watch_list[i]) {
            logger.warn('Ignore already watched symbol ' + symbol);
            return;
        }
    }

    this.watch_list.push(symbol);
};

User.prototype.removeFromWatchList = function(symbol) {
    var list = [];

    for (var i = 0; i < this.watch_list.length; i++) {
        if (symbol == this.watch_list[i]) {
            list = list.concat(this.watch_list.splice(0, i));
            list = list.concat(this.watch_list.splice(1, this.watch_list.length));
            this.watch_list = list;
            return;
        }
    }

    logger.warn('Ignore unwatched symbol ' + symbol);
};

User.prototype.save = function() {
    var self = this;

    return when.promise(function(resolve, reject) {
        // Hash password. We only save hashed password to database
        bcrypt.genSalt(10, function(err, salt) {
            if (err) {
                reject(err);
            }

            bcrypt.hash(self.password, salt, function(err, hashed_password) {
                if (err) {
                    reject(err);
                }

                self.password = hashed_password;
                resolve(userStore.saveUser(self, true));
            });
        });
    });
};

User.prototype.remove = function() {
    return userStore.removeUser(this, true);
};

User.prototype.comparePassword = function(password) {
    var self = this;

    return when.promise(function(resolve, reject) {
        bcrypt.compare(password, self.password, function(err, isMatch) {
            if (err) {
                reject(err);
            } else {
                resolve(isMatch);
            }
        });
    });
};

function validateProperty(property, value) {
    var re;

    switch (property) {
        case 'username':
            re = /^[a-z][\w\.]{0,24}$/i;
            if (!re.test(value)) {
                throw new Error('Invalid username format');
            }
            break;

        case 'email':
            re = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
            if (!re.test(value)) {
                throw new Error('Invalid email address');
            }
            break;

        case 'firstname':
        case 'lastname':
            if (!!value) {
                re = /^((?=[a-z \']).)+$/i;
                if (!re.test(value)) {
                    throw new Error('Invalid ' + property);
                }
            }
            break;

        case 'password':
            break;

        case 'id':
            throw new Error('Set ' + property + ' not allowed');
            break;

        default:
            throw new Error('Unknown user property: ' + property);
    }
}
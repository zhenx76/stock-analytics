var logger = require('../utility').logger;
var when = require('when');
var fs = require('fs');
var readline = require('readline');
var streamify = require('stream-array');
var os = require('os');

var AWS = require('aws-sdk');
AWS.config.region = 'us-west-1';
var s3 = new AWS.S3();

var UserStore = function(options) {
    if (!options.hasOwnProperty('dataStore')) {
        throw new Error('Must specify dataStore property');
    }

    this.options = options;
    this.users = [];
};

UserStore.prototype.loadUsers = function() {
    var self = this;
    if (self.options.dataStore == 's3') {
        var params = {Bucket: 'stock-analytics', Key: 'users.dat'};

        return when.promise(function(resolve, reject) {
            readline.createInterface({
                terminal: false,
                input: s3.getObject(params).createReadStream()
                    .on('error', function(err) {
                        if (err && err.code == 'NoSuchKey') {
                            logger.warn('User records does not exist');
                            resolve();
                        } else {
                            logger.error('Error loading user records: ' + JSON.stringify(err, null, 2));
                            reject();
                        }
                    })
                    .on('end', function() {
                        logger.info(self.users.length + ' user records loaded');
                        resolve();
                    })
            }).on('line', function(line) {
                var user = self.factory(JSON.parse(line));
                self.users.push(user);
            });
        });

    } else if (self.options.dataStore == 'fs') {
        return when.promise(function(resolve, reject) {
        });
    }
};

UserStore.prototype.setFactory = function(factoryMethod) {
    this.factory = factoryMethod;
};

UserStore.prototype.findUser = function(username) {
    var self = this;
    for (var i = 0; i < self.users.length; i++) {
        if (self.users[i].username == username) {
            return self.users[i].clone();
        }
    }
    return null;
};

UserStore.prototype.saveUser = function(user, sync) {
    var self = this;
    for (var i = 0; i < self.users.length; i++) {
        if (self.users[i].username == user.username) {
            self.users[i] = user; // replace old user
            break;
        }
    }

    if (i == self.users.length) { // add new user
        user.id = i;
        self.users.push(user);
    }

    if (sync) {
        return updateDataStore(self);
    } else {
        return when.resolve();
    }
};

UserStore.prototype.removeUser = function(user, sync) {
    var self = this;
    var users = [];

    for (var i = 0; i < self.users.length; i++) {
        if (self.users[i].username == user.username) {
            users = users.concat(self.users.splice(0, i));
            users = users.concat(self.users.splice(1, self.users.length));
            self.users = users;
        }
    }

    if (sync) {
        return updateDataStore(self);
    } else {
        return when.resolve();
    }
};

function updateDataStore(self) {
    if (self.options.dataStore == 's3') {
        var jsonArray = [];
        var totalLength = 0;
        self.users.forEach(function (user) {
            var userStr = JSON.stringify(user);
            jsonArray.push(userStr);
            totalLength += Buffer.byteLength(userStr);
        });
        jsonArray.push(os.EOL);
        totalLength += Buffer.byteLength(os.EOL);

        // Write data to S3
        // In order to upload any object to S3, we need to provide a Content-Length.
        // Typically, the SDK can infer the contents from Buffer and String data
        // (or any object with a .length property), and we have special detections
        // for file streams to get file length.
        // Unfortunately, there's no way the SDK can figure out the length of an arbitrary stream,
        // so if you pass something like an HTTP stream,
        // you will need to manually provide the content length yourself.
        var params = {
            Bucket: 'stock-analytics',
            Key: 'users.dat',
            Body: streamify(jsonArray),
            ContentLength: totalLength
        };

        return when.promise(function (resolve, reject) {
            s3.putObject(params, function (err, data) {
                if (err) {
                    logger.error('Error updating user records: ' + JSON.stringify(err, null, 2));
                    reject(err);
                } else {
                    logger.info(self.users.length + ' user records updated. ETag = ' + data.ETag);
                    resolve();
                }
            });
        });
    } else if (self.options.dataStore == 'fs') {
        return when.promise(function (resolve, reject) {
        });
    }
}

var s3Store = new UserStore({'dataStore' : 's3'});

module.exports = {
    userStore: s3Store
};
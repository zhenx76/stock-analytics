var jwt = require('jwt-simple');
var JwtStrategy = require('passport-jwt').Strategy;
var config = require('../config');
var User = require('../user-mgmt').User;

exports.config = function(passport) {
    var opts = {};
    opts.secretOrKey = config.secret;
    passport.use(new JwtStrategy(opts, function(jwt_payload, done) {
        var user = User.find(jwt_payload.username);
        if (user) {
            done(null, user);
        } else {
            done(null, false);
        }
    }));
};

exports.signup = function(req, res) {
    if (!req.body.username || !req.body.password) {
        res.json({success: false, msg: 'Please pass name and password.'});
    } else if (User.find(req.body.username)) {
        res.json({success: false, msg: 'Username ' + req.body.username + ' already exists.'});
    } else {
        try {
            var newUser = new User(req.body.username);
            newUser.set('password', req.body.password);
            newUser.set('email', req.body.email || '');
            newUser.set('firstname', req.body.firstname || '');
            newUser.set('lastname', req.body.lastname || '');

            // save the user
            newUser.save()
                .then(function() {
                    res.json({success: true, msg: 'Successful created new user.'});
                })
                .catch(function(err) {
                    return res.json({success: false, msg: err.message});
                });
        } catch (err) {
            return res.json({success: false, msg: err.message});
        }
    }
};

exports.authenticate = function(req, res) {
    if (!req.body.username || !req.body.password) {
        res.json({success: false, msg: 'Please pass name and password.'});
    } else {
        var user = User.find(req.body.username);
        if (!user) {
            res.send({success: false, msg: 'Authentication failed. User not found.'})
        } else {
            // check if password matches
            user.comparePassword(req.body.password)
                .then(function(isMatch) {
                    if (isMatch) {
                        // if user is found and password is right create a token
                        var token = jwt.encode(user, config.secret);

                        // return the information including token as JSON
                        res.json({success: true, token: 'JWT ' + token});
                    } else {
                        res.send({success: false, msg: 'Authentication failed. Wrong password.'});
                    }
                })
                .catch(function(err) {
                    res.send({success: false, msg: JSON.stringify(err, null, 2)});
                });
        }
    }
};

exports.getUserProfile = function(req, res) {
    var token = getToken(req.headers);
    if (token) {
        var decoded = jwt.decode(token, config.secret);
        var user = User.find(decoded.username);
        if (!user) {
            return res.status(403).send({success: false, msg: 'Authentication failed. User not found.'});
        } else {
            // Don't return password.
            // It's okay to delete the property because what we get from User.find()
            // is a cloned copy of the object
            delete user.password;
            res.json({success: true, msg: user});
        }
    } else {
        return res.status(403).send({success: false, msg: 'No token provided.'});
    }
};

exports.decodeUser = function(req, cb) {
    var token = getToken(req.headers);
    if (token) {
        var decoded = jwt.decode(token, config.secret);
        var user = User.find(decoded.username);
        if (!user) {
            cb(new Error('Authentication failed. User not found.'), null);
        } else {
            cb(null, user);
        }
    } else {
        cb(new Error('No token provided.'), null);
    }
};

function getToken(headers) {
    if (headers && headers.authorization) {
        var parted = headers.authorization.split(' ');
        if (parted.length === 2) {
            return parted[1];
        } else {
            return null;
        }
    } else {
        return null;
    }
};
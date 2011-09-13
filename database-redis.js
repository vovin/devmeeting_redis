var redis = require('redis'),

//client = redis.createClient(6379,"10.1.1.102");
client = redis.createClient();

//------------------------------------------------------------------------------

function Database(){
}

exports.Database = Database;

Database.prototype.selectTweets = function (username, callback) {
    
    this.selectUser(username, function(user) {        
        client.lrange("usertweets:"+ user, 0, 20, function(err, replies){ 
            if(replies.length === 0) callback(JSON.parse("[]"));
            else {
                client.mget(replies, function(err, r){
                    callback(r.map(function(t){
                        return JSON.parse(t);
                    }));
                });
            }
            
        });        
    });    
};

Database.prototype.insertTweet = function (username, status, callback) {
    var that = this;
    this.selectUser(username, function(u) {
        that.nextId("nextstatusid", function(newid) {
            var next = newid;
            var date = new Date();
            var gmt = date.toGMTString();
           
            client.set(
                "status:"+next, 
                JSON.stringify({
                    "text":status, 
                    "created_at":gmt,
                    "updated_at":gmt,
                    "user_id":u
                })
            );
            client.lpush("usertweets:"+u, "status:"+next);
            callback(status);
        });    
        
    });
};

Database.prototype.selectTimeline = function (username, callback) {
    this.selectUser(username, function(user) {   
        client.lrange("userhometimeline:"+ user, 0, 20, function(err, replies){  
            if(replies.length === 0) callback(JSON.parse("[]"));
            else {
                client.mget(replies, function(err, r){
                    callback(r.map(function(t){
                        return JSON.parse(t);
                    }));
                });
            }
        }); 
        
        
    });
};

Database.prototype.selectUser = function (username, callback) {
    client.get("userscreenname:"+username, function(err, user){        
        callback(user);   
    });
};

//------------------------------------------------------------------------------
Database.prototype.nextId = function (counter, callback) {
  client.incrby(counter, 1, function (err, reply) {
    callback(reply);
  });
};

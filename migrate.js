var redis = require('redis'), client = redis.createClient();
var mysql = require('mysql');
var Step = require('step');


var OFFSET = 10000;
var statusID = 0;


function migrateDb (mclient,dbName,done) {
	Step(
	function(){	
		migrateUsers(mclient,0,this.parallel());
		migrateStatuses(mclient,0,this.parallel());
	},
	function(err,res){
		migrateFollowers(mclient, 0, this);
	},
	function(err, res){
		console.log('DB '+dbName+' done!');
		this();
	},
	done
	);
}

function migrateUsers (mclient,offset,done) {
	mclient.query("SELECT * FROM users ORDER BY id LIMIT "+offset+","+OFFSET, function(err,res){
			if(err){
				throw err;
			}
			if(res && res.length==0){
				return done();
			}
			console.log('inserting Users with offset: '+offset);
			insertUsers(res,function(err,res){
				migrateUsers(mclient,offset+OFFSET,done);
			});
		});
}

function migrateStatuses (mclient,offset,done) {
	offset = offset || 0;
	mclient.query('SELECT * FROM statuses ORDER BY created_at DESC LIMIT '+offset+','+OFFSET, function(err, res){
		if(err){
			throw err;
		}
		if(res && res.length==0){
			return done();
		}
		console.log('inserting Statuses with offset: '+offset);
		insertStatuses(res,function(err,res){
			migrateStatuses(mclient,offset+OFFSET,done);
		});
	});
}

function migrateFollowers (mclient, offset, done) {
	offset = offset || 0;
	mclient.query('SELECT * FROM followers ORDER BY user_id ASC LIMIT ' +offset+','+OFFSET, function(err, res){
		if(err){
			throw err;
		}
		if(res && res.length == 0){
			return done();
		}
		console.log('inserting Followers with offset: '+offset);
		insertFollowers(res, function(err,res){
			migrateFollowers(mclient,offset+OFFSET,done);
		});
	});
}

function insertUsers(users,done){
	Step(
	function (){
		var group = this.group();
		users.forEach(function(u){
			client.set('user:'+u.id, JSON.stringify(u),group());
			client.set('userscreenname:'+u.screen_name, u.id,group());
		});
	},
	function (err){
		if(err){
			throw err;
		}
		process.nextTick(done);
		this();
	}
	);
}
function insertStatuses(statuses,done){
	Step(
	function (){
		var group = this.group();
		statuses.forEach(function(s){
			++statusID;
			s.id = statusID;
			client.set('status:'+statusID,JSON.stringify(s),group());
			client.rpush('usertweets:'+s.user_id,'status:'+statusID,group());
			});
	},
	function (err){
		if(err){
			throw err;
		}
		client.set('nextstatusid',statusID+1,this);
	},
	function (err){
		process.nextTick(done);
		this();
	});

}
function insertFollowers(followers,done){
	Step(
	function(){
		var group = this.group();
		followers.forEach(function(f){
			client.lpush('userfollowers:'+f.user_id,f.follower_id,group());
			var groupcallback2 = group();
			client.lpush('userfollowed:'+f.follower_id,f.user_id,function(){
				// set home line
				client.lrange('usertweets:'+f.user_id,0,20,function(err,tweets){
					client.lpush('userhometimeline:'+f.follower_id,tweets,groupcallback2);
					});
			});
		});
	},
	done
	);
}

var mysqlclients =[];
Step(
function (){
	var group = this.group();
	for (var i = 1,mclient; i <= 4; i++) {

		mclient = mysql.createClient({
			user: 'devcamp',
			password: 'devcamp',
			host: 'localhost',
			port: 3306,
			database: 'twitter'+i,
		});
		mysqlclients.push(mclient);
		migrateDb(mclient,'twitter'+i,group());
	};
},
fillAllUsersHomeline,
function (){
	console.log('all done!');
	mysqlclients.forEach(function(c){
		c.end();
	});
	client.quit();
});


var startedUsers=0,finishedUsers=0;
function writeHomelineStatus(){
	process.stdout.write('\rstartedUsers: '+startedUsers+' finished: '+finishedUsers+'      ');
}

function fillUserHomeline(userId,callback){
	Step(
	function (){
		startedUsers++;
		writeHomelineStatus();
		client.lrange('userfollowed:'+userId,0,-1,this);
	},
	function (err,followedIds){
		if(err){
			throw err;
		}
		if(followedIds.length==0){
			console.log('empty followed');
			return this();
		}
		var group = this.group();
		followedIds.forEach(function (fid){
			client.lrange('usertweets:'+fid,0,19,group());
			});
	},
	function (err,results){
		if(err){
			throw err;
		}
		if(!results){
			console.log('no tweets for user'+userId);
			return this();
		}
		var statuses = results.reduce(function(a,b){return a.concat(b)},[]);
		statuses.sort(statusSorter);
		if(statuses.length==0){
			return this();
		}
		
		client.lpush('userhomeline:'+userId,statuses,this);
	},
	function (err,res){
		if(err){
			throw err;
		}
		finishedUsers++;
		writeHomelineStatus();
		this();
	},
	callback
	);
}

function fillAllUsersHomeline(){
	var done = this;
	Step(
		function (){
			console.log('starting import of all users\n');
			client.keys('userfollowed:*',this);
		},
		function (err, userKeys){
			if(err){
				throw err;
			}
			var group = this.group();
			userKeys.forEach(function(key){
				key = key.split(':')[1];
				fillUserHomeline(key,group());
				});
		},
		function (err){
			if(err){
				throw err;
			}
			console.log('All done! :)');
			this();
		},
		done
		);
}


function statusSorter(a,b){
	a =  +a.split(':')[1];
	b =  +b.split(':')[1];
	return b-a;
}



var redis = require('redis'), client = redis.createClient();
var mysql = require('mysql');


var OFFSET = 10000;
var statusID = 0;
var workToDo;

function decreaseWork(){
	--workToDo;
}

function migrateDb (mclient) {
//	migrateUsers(mclient,0);
//	migrateStatuses(mclient,0);
	migrateFollowers(mclient, 0);
}

function migrateUsers (mclient,offset) {
	workToDo++;
	mclient.query("SELECT * FROM users ORDER BY id LIMIT "+offset+","+OFFSET, function(err,res){
			if(err){
				throw err;
			}
			if(res && res.length==0){
				return;
			}
			console.log('inserting Users with offset: '+offset);
			insertUsers(res);
			migrateUsers(mclient,offset+OFFSET);
			workToDo--;
		});
}

function migrateStatuses (mclient,offset) {
	offset = offset || 0;
	workToDo++;
	mclient.query('SELECT * FROM statuses ORDER BY created_at DESC LIMIT '+offset+','+OFFSET, function(err, res){
		if(err){
			throw err;
		}
		if(res && res.length==0){
			return;
		}
		console.log('inserting Statuses with offset: '+offset);
		insertStatuses(res);
		migrateStatuses(mclient,offset+OFFSET);
		workToDo--;
	});
}

function migrateFollowers (mclient, offset) {
	offset = offset || 0;
	workToDo++;
	mclient.query('SELECT * FROM followers ORDER BY user_id ASC LIMIT ' +offset+','+OFFSET, function(err, res){
		if(err){
			throw err;
		}
		if(res && res.length == 0){
			return;
		}
		console.log('inserting Followers with offset: '+offset);
		insertFollowers(res);
		migrateFollowers(mclient,offset+OFFSET);	
		workToDo--;
	});
}

function insertUsers(users){
	users.forEach(function(u){
		workToDo++;
		client.set('user:'+u.id, JSON.stringify(u),decreaseWork);
		workToDo++;
		client.set('userscreenname:'+u.screen_name, u.id,decreaseWork);
	});
}
function insertStatuses(statuses){
	statuses.forEach(function(s){
		++statusID;
		s.id = statusID;
		workToDo++;
		client.set('status:'+statusID,JSON.stringify(s),decreaseWork);
		workToDo++;
		client.rpush('usertweets:'+s.user_id,'status:'+statusID,decreaseWork);
		});
	workToDo++;
	client.set('nextstatusid',statusID+1,decreaseWork);
}
function insertFollowers(followers){
	console.log('insertFollowers ',followers.length);
	followers.forEach(function(f){
		workToDo++;
		client.lpush('userfollowers:'+f.user_id,f.follower_id,decreaseWork);
		workToDo++;
		client.lpush('userfollowed:'+f.follower_id,f.user_id,function(){
			// set home line
			workToDo++;
			client.lrange('usertweets:'+f.user_id,0,20,function(err,tweets){
				if(!err && tweets && tweets.length>0){
				client.lpush('userhometimeline:'+f.follower_id,tweets,function(er,res){
					console.log('follower_id is',f,f.follower_id,tweets.length);
					decreaseWork();
					});
				}
				});
			decreaseWork();
		});
	});
}

workToDo = 0;
var mysqlclients =[];
for (var i = 1,mclient; i <= 4; i++) {

	mclient = mysql.createClient({
		user: 'twitter',
		password: 'twpass',
		host: 'localhost',
		port: 3306,
		database: 'twitter'+i,
	});
	mysqlclients.push(mclient);
	migrateDb(mclient);
};
//checkNextWork();

function checkNextWork(){
	if(workToDo != 0){
		setTimeout(checkNextWork,500);
	}else{
		console.log('!!!!!!!!!!!! Next step !!!!!!!!');
		migrateFollowers(mysqlclients[0],0);
		migrateFollowers(mysqlclients[1],0);
		migrateFollowers(mysqlclients[2],0);
		migrateFollowers(mysqlclients[3],0);
		followersFinishedCheck();
	}
}
//followersFinishedCheck();

function followersFinishedCheck(){
	if(workToDo != 0){
		setTimeout(followersFinishedCheck,500);
	}else{
		console.log('Followers finished, starting sorting');
		client.keys('userhometimeline:*',function(err, keys){
			keys.forEach(function(ht){
				workToDo++;
				client.lrange(ht,0,-1,function(err,statusIDs){
					client.del(ht,function(err){
						client.lpush(ht,statusIDs.sort().reverse().slice(0,20),decreaseWork);
						});
					});
				});
		});
		//endAppCheck();
	}
}

function endAppCheck(){
	if(workToDo != 0){
		setTimeout(endAppCheck,500);
	}else{
		console.log('done!');
		process.exit();
	}
}

/*
function getUserIDs(callback){
	client.keys('user:*',function(err,res){
		if(err){
			throw err;
		}
		callback(res);
	});
}

function processUsersTimeline(users){
	users.forEach(function(uid){
		processTimelineForUser(uid.split(':')[1]);
		});
}

function processTimelineForUser(userID){
	client.lrange('userfollowed:'+userID,0,-1,function(err, followedIDs){
		followedIDs = followedIDs.map(function(el){return 'usertweets:'+el;});
		
	});
}
*/



var redis = require('redis'), client = redis.createClient();


var OFFSET = 10000;
var statusID = 0;
var workToDo;



workToDo = 0;
followersFinishedCheck();

function followersFinishedCheck(){
		console.log('Followers finished, starting sorting');
		client.keys('userhometimeline:*',function(err, keys){
			keys.forEach(function(ht){
				workToDo++;
				client.lrange(ht,0,-1,function(err,statusIDs){
					client.del(ht,function(err){
						client.lpush(ht,statusIDs.sort(statusSorter).slice(0,20));
						});
					});
				});
		});
}

function statusSorter(a,b){
	a =  +a.split(':')[1];
	b =  +b.split(':')[1];
	return b-a;
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



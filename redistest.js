var redis = require('redis'),
	client = redis.createClient();

client.lpush('l',5, redis.print);
client.lpush('l',7, redis.print);
client.lpush('l',8, redis.print);
client.lpush('l',4, function(){
	console.log('ok');
});


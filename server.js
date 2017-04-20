    var express = require('express');
var app = express();
var mongoose = require('mongoose').Mongoose;
var pythonShell = require('python-shell');

var pulses = new mongoose();
pulses.connect('mongodb://localhost:27017/pulses');
var CurrentSummationDelivered = pulses.model('CurrentSummationDelivered', {
	value : Number,
	when : Date
}, 'CurrentSummationDelivered');
var InstantaneousDemand = pulses.model('InstantaneousDemand', {
	value : Number,
	when : Date
}, 'InstantaneousDemand');
var RmsCurrent = pulses.model('RmsCurrent', {
	value : Number,
	when : Date
}, 'RmsCurrent');
var Voltage = pulses.model('Voltage', {
	value : Number,
	when : Date
}, 'Voltage');
var PowerFactor = pulses.model('PowerFactor', {
	value : Number,
	when : Date
}, 'PowerFactor');
var Averages = pulses.model('Averages', {
	currentSummationDelivered : Number,
	instantaneousDemand : Number,
	rmsCurrent : Number,
	voltage : Number,
	powerFactor : Number,
	hoursAgo : Number
}, 'Averages');
var Daily = pulses.model('Daily', {
	date : Date,
	turnedOn : Number,
	timeOn : Number,
	timeOff : Number,
	timeSleep : Number,
	timeLowLoad : Number,
	timeHighLoad : Number,
	weekday : Number
}, 'Daily');

app.get('/api/currentSummationDelivered', function(req, res) {
	CurrentSummationDelivered.find(function(err, db) {
		if (err) {
			res.send(err);
		}

		res.json(db);
	});
});

app.get('/api/instantaneousDemand', function(req, res) {
	InstantaneousDemand.find(function(err, db) {
		if (err) {
			res.send(err);
		}

		res.json(db);
	});
});

app.get('/api/rmsCurrent', function(req, res) {
	RmsCurrent.find(function(err, db) {
		if (err) {
			res.send(err);
		}

		res.json(db);
	});
});

app.get('/api/voltage', function(req, res) {
	Voltage.find(function(err, db) {
		if (err) {
			res.send(err);
		}

		res.json(db);
	});
});

app.get('/api/powerFactor', function(req, res) {
	PowerFactor.find(function(err, db) {
		if (err) {
			res.send(err);
		}

		res.json(db);
	});
});

app.get('/api/averages', function(req, res) {
	Averages.find(function(err, db) {
		if (err) {
			res.send(err);
		}

		res.json(db);
	});
});

app.get('/api/daily', function(req, res) {
	Daily.find(function(err, db) {
		if (err) {
			res.send(err);
		}

		res.json(db);
	});
});

app.get('/', function(req, res) {
	var options = {
		mode: 'text',
		pythonPath: '/usr/bin/python',
		scriptPath: '/home/calplug/analysis'
	};
	pythonShell.run('voltronAnalysis.py', options, function(err, results) {
		if (err) throw err;

		console.log("results: %j", results);

		res.sendFile('/home/calplug/voltron/index.html'); 

	});
});


app.use(express.static('voltron')); 


app.listen(3000);

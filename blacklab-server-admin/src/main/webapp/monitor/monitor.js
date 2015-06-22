// Shim Date.now() (for pre-ES5 browsers)
if (!Date.now) {
    Date.now = function() { return new Date().getTime(); };
}

function dispTime(ms) {
	var s = Math.floor(ms / 1000);
	var f = ms % 1000;
	var a = Math.floor(f / 100);
	var b = Math.floor(f / 10) % 10;
	return s + "." + a + b;
}

function report(jqXHR, textStatus) {
	var message = "";
	if (jqXHR.status != 0 || jqXHR.statusText != "error") {
		message = jqXHR.status + " " + jqXHR.statusText;
	}
	if (textStatus != "error") {
		if (message.length > 0)
			message += "; ";
		message += textStatus;
	}
	if (message.length == 0) {
		message = "unknown error, possibly Same Origin Policy violation";
	}
	return message;
}

function clearCache() {
	$.ajax("/blacklab-server/cache-clear", {
		"type": "POST",
		"accept": "application/json",
		"dataType": "json",
		"success": function (data) {
			// OK
		},
		"error": function (jqXHR, textStatus, errorThrown) {
			var data = jqXHR.responseJSON;
			var message = textStatus;
			if (data && data['error'])
				message = data['error']['message'];
			alert("Error clearing cache: " + report(jqXHR, message));
		},
	});
}

function onChangeFinishedSetting() {
	finishedSetting = $("#hideFinished").val();
	if (lastCacheInfo)
		updateJobTable();
}

function onChangeWaitingSetting() {
	waitingSetting = $("#hideWaiting").val();
	if (lastCacheInfo)
		updateJobTable();
}

function makeUrl(searchParam) {
	var parts = [];
	var resource = "hits";
	for (key in searchParam) {
		if (key == "jobclass") {
			if (searchParam[key].indexOf("Doc") >= 0)
				resource = "docs";
		} else if (searchParam.hasOwnProperty(key) && key != "indexname" && key != "jobclass") {
			parts.push(key + "=" + searchParam[key]);
		}
	}
	parts.sort();
	return resource + "?" + parts.join("&"); 
}

var HIDE_CACHED_AFTER = 20;

function messageRow(msg) {
	return "<tr><td colspan='10' class='message'>" + msg + "</td></tr>";
}

var lastCacheInfo;

var finishedSetting;

var waitingSetting;

function dispTime(s) {
	return Math.floor(s * 100) / 100.0;
}

function updateJobTable() {
	var data = lastCacheInfo;
	var rows = [];
	var hidden = 0;
	for (var i = 0; i < data.cacheContents.length; i++) {
		var job = data.cacheContents[i];
		
		if (finishedSetting != 'show' && job.stats.status == "finished" && (finishedSetting == 'hide' || job.stats.notAccessedFor >= HIDE_CACHED_AFTER) ) {
			hidden++;
			continue;
		}
		
		var indexName = job.searchParam.indexname;
		var url = makeUrl(job.searchParam);
		/*
		var patt = job.searchParam.patt;
		if (!patt)
			patt = "";
		var meta = job.searchParam.filter;
		if (!meta)
			meta = "";*/
		var className = job.searchParam.jobclass;
		var type = job.stats.type;
		if (job.searchParam.number == 0) {
			type = "status check";
		} else if (type == "search") {
			switch(job.searchParam.jobclass) {
			case "JobHits":
				type = "find hits";   break;
			case "JobDocs":
				type = "find docs";   break;
			case "JobHitsGrouped":
				type = "group hits";  break;
			case "JobDocsGrouped":
				type = "group docs";  break;
			case "JobHitsSorted":
				type = "sort hits"; break;
			case "JobDocsSorted":
				type = "sort docs"; break;
			case "JobHitsWindow":
				type = "fetch page"; break;
			}
		} else if (type == "count") {
			type = "count hits";
		}
		var status = job.stats.status;
		var userWait = job.stats.userWaitTime;
		var execTime = job.stats.totalExecTime;
		var notAccessedFor = job.stats.notAccessedFor;
		var pausedFor = job.stats.pausedFor;
		var refsToJob = job.stats.refsToJob;
		var waitingForJobs = job.stats.waitingForJobs;
		if (waitingForJobs > 0) {
			if (waitingSetting != 'show') {
				hidden++;
				continue;
			}
			status = "waiting";
		}
		rows.push(["<tr onclick='showJobDetail(", job.id,");'><td>", job.id, "</td><td>", 
		        indexName, "</td><td>", url, "</td><td>", type, "</td><td>", 
				status, "</td><td>", dispTime(userWait), "</td><td>", dispTime(execTime), "</td><td>", 
				dispTime(notAccessedFor), "</td><td>", dispTime(pausedFor), "</td><td>", 
				refsToJob, "</td></tr>"].join(""));
	}
	if (rows.length == 0) {
		var msg = hidden == 0 ? "The cache is empty." : "No jobs to show here (" + hidden + " hidden).";
		rows.push(messageRow(msg));
	} else {
		rows.sort();
		if (hidden > 0)
			rows.push(messageRow("(" + hidden + " jobs hidden)"));
	}
	$("#tableBody").html(rows.join(""));
}

function dump(obj) {
	
	function arrayDump(obj) {
		var html = ["<table>"];
		for (var i = 0; i < obj.length; i++) {
			html.push("<tr><td>", dump(obj[i]), "</td></tr>");
		}
		html.push("</table>");
		return html.join("");
	}

	function objDump(obj) {
		var html = ["<table>"];
		for (key in obj) {
			if (obj.hasOwnProperty(key)) {
				html.push("<tr><th>", key, "</th><td>", dump(obj[key]), "</td></tr>");
			}
		}
		html.push("</table>")
		return html.join("");
	}

	if (Object.prototype.toString.call(obj) === '[object Array]') {
	    return arrayDump(obj);
	}
	if (typeof obj == 'object')
		return objDump(obj);
	return obj.toString();
}

var jobToShowDetailOf = -1;

function showJobDetail(id) {
	jobToShowDetailOf = id;
	updateJobDetail();
}

function updateJobDetail() {
	var id = jobToShowDetailOf;
	$("#jobDetail").html("");
	var cache = lastCacheInfo.cacheContents;
	for (var i = 0; i < cache.length; i++) {
		var job = cache[i];
		if (job.id == id) {
			$("#jobDetailHeading").text("Details for job " + job.id);
			$("#jobDetail").html(dump(job.debugInfo));
			break;
		}
	}
}

function update() {
	var blsUrl = $("#blsUrl").val();
	
	$.ajax(blsUrl + "/cache-info?debug=true", {
		"accept": "application/json",
		"dataType": "json",
		"success": function (data) {
			lastCacheInfo = data;
			updateJobTable();
			updateJobDetail();
			setTimeout(update, 1000);
		},
		"error": function (jqXHR, textStatus, errorThrown) {
			var message = textStatus;
			var data = jqXHR.responseJSON;
			if (data && data['error'])
				message = data['error']['message'];
			$("#tableBody").html(messageRow("ERROR: " + report(jqXHR, message)));
			//alert("ERROR: " + report(jqXHR, message));
			
			setTimeout(update, 1000);
		},
	});
}

$(document).ready(function () {
	$("#cache").tablesorter();
	onChangeFinishedSetting();
	onChangeWaitingSetting();
	update();
});

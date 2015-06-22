// Shim Date.now() (for pre-ES5 browsers)
if (!Date.now) {
    Date.now = function() { return new Date().getTime(); };
}

// Show test output
function output(msg) {
	var con = $("#output");
	con.append(msg + "\n");
	con.scrollTop(con[0].scrollHeight - con.height());
}

function rnd(n) {
	return Math.floor(Math.random() * n);
}

function chance(ch) {
	return Math.random() < ch;
}

function choose(arr) {
	return arr[rnd(arr.length)];
}

function dispTime(ms) {
	var s = Math.floor(ms / 1000);
	var f = ms % 1000;
	var a = Math.floor(f / 100);
	var b = Math.floor(f / 10) % 10;
	return s + "." + a + b;
}

function traceQueries() {
	return !!($("#traceQueries")[0].checked);
}

var queryComplexity = 0;

function onChangeQueryComplexity() {
	queryComplexity = parseInt($("#queryComplexity").val());
}


var simpleClauses = [
    "[lemma='hond']",
    "[lemma='varken']",
    "[lemma='water']",
    "[lemma='zee']",
    "[lemma='schip']",
    "[lemma='land']",
    "[lemma='man']",
    "[lemma='vrouw']",
    "[lemma='moeder']",
    "[lemma='vader']",
    "[lemma='broer|broeder']",
    "[lemma='zus|zuster']",
    "[lemma='mens']",
    "[lemma='iemand']",
    "[lemma='huis']",
    "[lemma='haard']",
    "[lemma='kerk']",
    "[lemma='sint']",
    "[lemma='mis']",
    "[lemma='weg']",
    "[lemma='rood']",
    "[lemma='zijn']",
    "[lemma='blijven']",
    "[lemma='komen']",
    "[lemma='willen']",
    "[lemma='wensen']",
    "[lemma='zien']",
    "[lemma='horen']",
    "[lemma='bidden']",
    "[lemma='lopen']",
    "[lemma='zitten']",
    "[lemma='liggen']"
];

var heavyClauses = [
    "[]",
    "[pos='N.*']",
    "[pos='V.*']",
    "[pos='A.*']",
    "[pos='C.*']",
    "[pos='P.*']",
    "'a.*'",
    "'c.*'",
    "'d.*'",
    "'e.*'",
    "'g.*'",
    "'k.*'",
    "'l.*'",
    "'m.*'",
    "'n.*'",
    "'o.*'",
    "'p.*'",
    "'r.*'",
    "'s.*'",
    "'t.*'",
];

var articles = [
	"de",
	"het",
	"een",
];

function repeat() {
	var min = rnd(2);
	var max = min + 1 + rnd(3);
	return "{" + min + "," + max + "}";	
}

function simpleClause() {
	return choose(simpleClauses);
}

function heavyClause() {
	return choose(heavyClauses);
}

function thing() {
	return "[lemma='" + choose(articles) + "'] [pos='ADJ.*']{0,3} [pos='N.*']";
}

function matchAll() {
	return "[]";
}

function preposition() {
	return "[pos='ADP.*']";
}

/*
 * Regels:
 * + max. 1 simple clause per query; met simple clause iha sneller dan zonder
 * + elke clause heeft een bepaalde kans om door een voorzetsel/matchall gevolgd te worden
 *   (misschien gewoon met bepaalde kans inserten in query?)
 * + hier en daar een "thing" (telt bijv. voor 2 clauses)
 * + non-simple, non-thing clause heeft bepaalde kans om een repeat te krijgen
 */

function randomPattern() {
	
	if (queryComplexity == -1) {
		return simpleClause();
	}
	
	var maxClauses = 3 + queryComplexity;
	var numClauses = rnd(maxClauses) + 1;             // 1 .. maxClauses  clauses
	var includeSimpleClause = chance(0.8 - queryComplexity * 0.15);   // including simple clause (usually) makes query faster
	var simpleClausePos = 
		includeSimpleClause ? rnd(numClauses) : -1;   // where the simple clause goes
		
	var relChanceThing = 1;
	var relChancePreposition = 1;
	var relChanceHeavy = 2 + queryComplexity;
	var relChanceMatchAll = 1;
	var chanceRepeat = 0.19 + 0.03 * queryComplexity;
	
	var relChanceTotal = relChanceThing + relChancePreposition + relChanceHeavy + relChanceMatchAll;
	
	var query = "";
	for (var i = 0; i < numClauses; i++) {
		if (i == simpleClausePos)
			query += simpleClause();
		else {
			var x = rnd(relChanceTotal);
			var repeatPossible = false;
			if (x < relChanceThing) {
				if (i + 2 < numClauses) {
					query += thing();
					i += 2; // (counts for 3 clauses)
				} else {
					query += heavyClause();
				}
			} else if (x < relChanceThing + relChancePreposition) {
				query += preposition();
			} else if (x < relChanceThing + relChancePreposition + relChanceHeavy) {
				query += heavyClause();
				repeatPossible = true; 
			} else {
				query += matchAll();
				repeatPossible = true; 
			}
			if (repeatPossible && chance(chanceRepeat)) {
				query += repeat();
			}
		}
		query += " ";
	}
	return query;
}

function optSort() {
	var odds = (7 - queryComplexity * 2);
	if (rnd(odds) == 0) {
		if (rnd(2) == 0) {
			return "left"; // sort on left context
		}
		return "field:title";
	}
	return null;
}

function optGroup() {
	var odds = (7 - queryComplexity * 2);
	if (rnd(odds) == 0) {
		if (rnd(2) == 0) {
			return "wordleft"; // group on word left of hit
		}
		return "field:title";
	}
	return null;
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

var queryNum = 0;

function clearCache() {
	output("Clearing BLS cache...");
	$.ajax("/blacklab-server/cache-clear", {
		"type": "POST",
		"accept": "application/json",
		"dataType": "json",
		"success": function (data) {
			output("Cache cleared succesfully.");
		},
		"error": function (jqXHR, textStatus, errorThrown) {
			var data = jqXHR.responseJSON;
			var message = textStatus;
			if (data && data['error'])
				message = data['error']['message'];
			output("Error clearing cache: " + report(jqXHR, message));
		},
	});
}

// Run the test with the given parameters
function runTest() {
	var blsUrl = $("#blsUrl").val();
	var queryFreq = $("#queryFreq")[0].value;
	var avgWaitBetweenQueriesMs = (1 / queryFreq) * 1000;
	//var minWaitBetweenQueriesMs = avgWaitBetweenQueriesMs * 0.5;
	var testDurationMs = $("#testDuration")[0].value * 1000;
	var testRunningSince = Date.now();
	var fireQueryAt = testRunningSince;
	
	var queryTimes = [];
	
	var nFired = 0;                 // number of queries fired so far
	var nFailure = 0;               // number of errors received
	var nSuccess = 0;               // number of succesful responses received
	var totalSuccesTime = 0;        // sum of the query times of succesful queries
	var queriesInProgress = 0;      // # queries currently in progress (#QIP)
	var maxQueriesInProgress = 0;   // max. # queries that were ever in progress
	var totalQueriesInProgress = 0; // every time a query is fired, the #QIP is added to this var.
	                                // this allows us to calculate the average #QIP
	$("#queriesFired").val(nFired);
	$("#queryFail").val(nFailure);
	$("#querySuccess").val(nSuccess);
	$("#averageResponse").val(0);
	$("#queriesInProgress").val(queriesInProgress);
	$("#avgQueriesInProgress").val(0);
	var allFired = false;
	
	$("#results").html(""); // clear previous result table
	
	// Fire off a query and set up trigger for the next query
	function query() {
		var patt = randomPattern();
		
		var data = {
			"waitfortotal": true,
			"patt": patt,
			"queryNum": ++queryNum  // avoid caching
		}
		var sort = null;
		if (rnd(2))
			sort = optSort();
			group = optGroup();
		if (sort)
			data['sort'] = sort;
		if (group)
			data['group'] = group;
		var startedAt = Date.now();
		
		$.ajax(blsUrl + "/hits", {
			"accept": "application/json",
			"dataType": "json",
			"data": data,
			"success": function (data) {
				nSuccess++;
				queriesInProgress--;
				$("#queriesInProgress").val(queriesInProgress);
				$("#querySuccess").val(nSuccess);
				var summary = data['summary'];
				var numHits = summary['numberOfHits'];
				var time = Date.now() - startedAt;
				totalSuccesTime += time;
				$("#averageResponse").val(dispTime(totalSuccesTime / nSuccess));
				var query = patt;
				if (sort)
					query += ", sort by " + sort;
				if (group)
					query += ", group by " + group;
				if (queryTimes.length < 300) {
					queryTimes.push({"query": query, "time": time, "hits": numHits});
				}
				if (traceQueries())
					output(query + ": " + dispTime(time) + "s, " + numHits + " hits");
				if (allFired && nSuccess + nFailure == nFired) {
					output("Done!");
					queryTimes.sort(function (a, b) {
						return a.time - b.time;
					});
					var resultsRows = [];
					$(queryTimes).each(function (index, value) {
						resultsRows.push("<tr><td class='right'>", dispTime(value.time), "</td><td>", value.query, "</td><td class='right'>", value.hits, "</td></tr>");
					});
					$("#results").html(resultsRows.join(""));
		            // let the plugin know that we made a update 
		            $("table.results").trigger("update"); 	
				}
			},
			"error": function (jqXHR, textStatus, errorThrown) {
				var message = textStatus;
				nFailure++;
				queriesInProgress--;
				$("#queriesInProgress").val(queriesInProgress);
				$("#queryFail").val(nFailure);
				var data = jqXHR.responseJSON;
				if (data && data['error'])
					message = data['error']['message'];
				output(patt + ": FAILURE, " + report(jqXHR, message));
				if (allFired && nSuccess + nFailure == nFired)
					output("Done!");
			},
		});
		//if (traceQueries()) output(patt + ": sent");
		nFired++;
		$("#queriesFired").val(nFired);
		queriesInProgress++;
		if (queriesInProgress > maxQueriesInProgress) {
			maxQueriesInProgress = queriesInProgress;
			$("#maxQueriesInProgress").val(maxQueriesInProgress);
		}
		$("#queriesInProgress").val(queriesInProgress);
		totalQueriesInProgress += queriesInProgress;
		$("#avgQueriesInProgress").val(Math.floor(totalQueriesInProgress / nFired * 10 ) / 10.0);
	}
	
	function checkFireQuery() {
		var now = Date.now();
		// Time to fire a query?
		if (now >= fireQueryAt) {
			// Yes
			query();
			
			// Done, or schedule next query?
			if (now >= testRunningSince + testDurationMs) {
				allFired = true;
			} else {
				fireQueryAt += Math.random() * avgWaitBetweenQueriesMs * 2;
			}
		}
		if (!allFired) {
			setTimeout(checkFireQuery, 100);
		}
	}
	checkFireQuery();
}

$(document).ready(function () {
	$("table.results").tablesorter(); 
	onChangeQueryComplexity();
});
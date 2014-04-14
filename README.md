BlackLab Server
===============

A REST API for BlackLab that speaks JSON and XML.

The goal for this webservice is to expose all of BlackLab's functionality through an easy-to-consume web service accessible from your favourite programming language.

BlackLab Server is currently in alpha, expected to go into beta this May.


Some simple example URLs:

Find hits for a word:

  http://corpus.example.com/blacklab-server/mycorpus/hitset?patt="easy"

Group hits by left context:

  http://corpus.example.com/blacklab-server/mycorpus/hitsgrouped?patt="easy"&groupby=left

Highlight in original content:

  http://corpus.example.com/blacklab-server/mycorpus/doc/12345/?patt="easy"


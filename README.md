BlackLab Server
===============

A REST API for BlackLab that speaks JSON and XML.

**WARNING: This is not yet production-ready software! This is an alpha version, available for review and testing. There are likely still be bugs and the protocol may change slightly. **

The goal for this webservice is to expose all of BlackLab's functionality through an easy-to-consume web service accessible from your favourite programming language.

Some simple example URLs:

Find hits for a word:

  http://corpus.example.com/blacklab-server/mycorpus/hitset?patt="easy"

Group hits by left context:

  http://corpus.example.com/blacklab-server/mycorpus/hitsgrouped?patt="easy"&groupby=left

Highlight in original content:

  http://corpus.example.com/blacklab-server/mycorpus/doc/12345/?patt="easy"


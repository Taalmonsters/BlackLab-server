BlackLab Server
===============

A webservice that allows you to use corpus search engine BlackLab from any programming language.

**NOTE: this is a beta version of BlackLab Server. It works well, but there may be occasional bugs. Please report any problems you encounter using the issue tracker. **

Please see the [full overview](https://github.com/INL/BlackLab-server/wiki/BlackLab-Server-overview) of the webservice for more information.

Here's some simple example URLs:

Find hits for a word:

  http://corpus.example.com/blacklab-server/mycorpus/hits?patt="easy"

Group hits by left context:

  http://corpus.example.com/blacklab-server/mycorpus/hits?patt="easy"&group=left

Highlight in original content:

  http://corpus.example.com/blacklab-server/mycorpus/docs/12345/?patt="easy"


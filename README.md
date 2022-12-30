pengyun-utils
=============

pengyun-utils

DIH tool Guide

Building

1. Run "mvn clean install" in pengyun-utils.
2. Then into target folder in pengyun-utils, and you can see a jar named  pengyun-utils-1.0.0-jar-with-dependencies.jar.
3. That is what we want.

Using

1. As the jar name is to long, you can run "mv pengyun-utils-1.0.0-jar-with-dependencies.jar DIH.jar" to rename the jar.
2. Then run "java -jar DIH.jar" and you can get all instances from local dih and results are showed in the comman line.
3. If you want to get instances from remote host, you can run command like this "java -jar DIH.jar 10.0.1.112:10000"("10.0.1.112" is the remote hostname).
===========

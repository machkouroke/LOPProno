import os
with open("build.sbt") as f:
    # print(f.readlines())
    lines: list[str] = [x for x in f.readlines() if x.startswith("ThisBuild")]
    versionJar: str = lines[0].split(":=")[1].strip().strip('"')
    versionScala: str = lines[1].split(":=")[1].strip().strip('"')
    print(versionJar, versionScala, sep="\n")

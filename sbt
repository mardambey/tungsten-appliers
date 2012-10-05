java -Xms1G -Xmx12G -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=384M -jar `dirname $0`/lib/sbt-launch.jar "$@"


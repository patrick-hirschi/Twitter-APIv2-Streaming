restart=""
while true; do
    gtimeout 600 python3 /Users/ph/Projects/twitter_spark_streaming/twitter_api_stream.py $restart
    restart="restart"
done 

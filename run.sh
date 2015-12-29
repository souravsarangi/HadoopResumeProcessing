if [ -z "$1" ]
then
    echo "No Proper argument"
else
    java -cp "lib/*:" ProcessResume $1 $2
fi

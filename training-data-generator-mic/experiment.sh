echo "FCFS" >> output.txt
./trials_simulator simple_cluster.xml deployment_cluster.xml >> output.txt
cat logs/lateness-log.txt >> output.txt
echo "--------------------------------" >> output.txt
echo " " >> output.txt

echo "SPT" >> output.txt
./trials_simulator simple_cluster.xml deployment_cluster.xml -spt >> output.txt
cat logs/lateness-log.txt >> output.txt
echo "--------------------------------" >> output.txt
echo " " >> output.txt

echo "LPT" >> output.txt
./trials_simulator simple_cluster.xml deployment_cluster.xml -lpt >> output.txt
cat logs/lateness-log.txt >> output.txt
echo "--------------------------------" >> output.txt
echo " " >> output.txt

echo "EDD" >> output.txt
./trials_simulator simple_cluster.xml deployment_cluster.xml -edd >> output.txt
cat logs/lateness-log.txt >> output.txt
echo "--------------------------------" >> output.txt
echo " " >> output.txt

echo "WFP3" >> output.txt
./trials_simulator simple_cluster.xml deployment_cluster.xml -wfp3 >> output.txt
cat logs/lateness-log.txt >> output.txt
echo "--------------------------------" >> output.txt
echo " " >> output.txt

echo "UNICEF" >> output.txt
./trials_simulator simple_cluster.xml deployment_cluster.xml -unicef >> output.txt
cat logs/lateness-log.txt >> output.txt
echo "--------------------------------" >> output.txt
echo " " >> output.txt

echo "c1" >> output.txt
./trials_simulator simple_cluster.xml deployment_cluster.xml -c1 >> output.txt
cat logs/lateness-log.txt >> output.txt
echo "--------------------------------" >> output.txt
echo " " >> output.txt

echo "c2" >> output.txt
./trials_simulator simple_cluster.xml deployment_cluster.xml -c2 >> output.txt
cat logs/lateness-log.txt >> output.txt
echo "--------------------------------" >> output.txt
echo " " >> output.txt

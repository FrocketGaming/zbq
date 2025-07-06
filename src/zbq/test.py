from zbq import zclient

print(zclient.bq(action="read", query="SELECT * FROM `gcplearning-392112.Books.Books_Read` LIMIT 1000"))

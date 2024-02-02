import json


people_string = '''
{
"people" : 
  {
   "name": "john",
   "phone": "56471587847",
   "email": ["ajnalkmkmpk@gmail.com","dkjfoksdj@gmail.com"]  
  }
  
}
'''
try:
  data = json.loads(people_string)
  #print(data)
  data2 = json.dumps(data,indent=2)
  print(data2)
 
except Exception as e:
  print(e)      

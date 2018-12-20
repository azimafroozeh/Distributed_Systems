# import requests
# # response=requests.get('http://localhost:8000/key_values_split_8.txt')
# # print(response.text)
with open("Output.txt") as f:
    text=f.read()
print(eval(text))
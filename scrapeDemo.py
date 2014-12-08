import urllib2
import requests
from bs4 import BeautifulSoup

client = requests.Session()

HOMEPAGE_URL = 'https://www.linkedin.com'
LOGIN_URL = 'https://www.linkedin.com/uas/login-submit'

html = client.get(HOMEPAGE_URL).content
soup = BeautifulSoup(html)
csrf = soup.find(id="loginCsrfParam-login")['value']

login_information = {
    'session_key':'victorpham93@gmail.com',
    'session_password':'minhviet93',
    'loginCsrfParam': csrf,
}

client.post(LOGIN_URL, data=login_information)

url = "https://www.linkedin.com/pub/alex-russell/0/842/320"
html_page = client.get(url)

soup = BeautifulSoup(html_page.text)

print "=========Endorse people==========="
endorsedDict = {"endorsements-received":"referrer", "endorsements-given":"recommendee"}

for (key, value) in endorsedDict.iteritems():
	if key == "endorsements-received":
		print "People endorsed me:"
	else:
		print "People I endorsed:"
	print key
	endorsementsReceived = soup.find("div", class_=key).find_all("div", class_="endorsement-info")
	for endorsed in endorsementsReceived:
		endorsedLink = endorsed.find("span",attrs={"data-tracking": value}).find("a")["href"]
		payload = {'authType': 'name', 'trk': 'miniprofile-name-link'}
		r = client.get(endorsedLink, params=payload).text.encode('utf-8')
		endorsedProfile = BeautifulSoup(r)
		print endorsedProfile.find("dl",class_="public-profile").find("a").string

print "=========Endorse skill==========="
skills = soup.find("ul", class_="skills-section").find_all("li", class_="endorse-item")
for skill in skills:
	skillName = skill.find("a", class_="endorse-item-name-text").string
	print skillName + " is endorsed by these people:"

	endorsers = skill.find("div", class_="endorsers-container").find_all("span", class_="new-miniprofile-container")
	for endorser in endorsers:
		miniProfileLink = endorser['class'][1]
		fullName = client.get("https://www.linkedin.com" + miniProfileLink).json()["content"]["MiniProfile"]["full_name"]
		print fullName


print "===========Similar people============="
memberId = soup.find("div", class_="masthead")["id"]
memberIdNo = memberId[(memberId.index('-') + 1):]
similarPeople = soup.find("ol", class_="discovery-results").find_all("li")
similarPeopleSet = set()
#print first 4 loaded people
for person in similarPeople:
	name = person.find("img")["alt"]
	similarPeopleSet.add(name)
for i in range(1,8):
	payload = {"offset":i*4, "records": 8, "id": memberIdNo}
	response = client.post("https://www.linkedin.com/profile/profile-v2-right-top-discovery-teamlinkv2", data=payload).json()

	for person in response["content"]["RightTop"]["discovery"]["people"]:
		similarPeopleSet.add(person["fullName"])

print(", ".join(str(e) for e in similarPeopleSet))



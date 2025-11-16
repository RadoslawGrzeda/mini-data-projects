from faker import Faker
fake =Faker("Pl")

def generate_person():
    person={
        "firstName":fake.first_name(),
        "lastName":fake.last_name(),
        "address":fake.address(),
        "email":fake.email(),
        "phoneNumber":fake.phone_number(),
        "dateOfBirth":fake.date_of_birth(),
        "sex": "female" if fake.first_name().endswith("a") else "male",
        # "firstName":fake.first_name()
    }
    person['dateOfBirth']=str(person['dateOfBirth'])
    return person;
# print(generate_person())

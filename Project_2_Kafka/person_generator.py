from faker import Faker
from datetime import datetime

fake = Faker('pl')


def generate_person():
    return {
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'date_of_birth': fake.date_of_birth().isoformat(),
        'gender': 'female' if fake.first_name().endswith('a') else 'male',
        'registration_date': datetime.now().isoformat(),
        'creation_application': 'person_generator.py'
        
    }
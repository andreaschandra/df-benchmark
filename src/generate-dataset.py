import pandas as pd
from tqdm.contrib.concurrent import process_map  # or thread_map
from faker import Faker

fake = Faker()

def generate_profile(idx):
    row = fake.profile(fields=['username', 'birthdate', 'sex', 'blood_group', 'mail', 'ssn', 'job', 'company',])

    return row

if __name__ == '__main__':
    r = process_map(generate_profile, range(10000000), max_workers=14, chunksize=1000)
    result = list(r)
    d_result = pd.DataFrame(result)
    d_result.to_csv('result.csv', index=False)
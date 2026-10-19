import csv
import json
import random
from datetime import datetime, timedelta

random.seed(42)

# ── Reference data ──────────────────────────────────────────────────
DEPARTMENTS = ["Cardiology", "Orthopedics", "Neurology", "Pediatrics",
               "Dermatology", "Oncology", "Psychiatry", "General Practice"]

BLOOD_TYPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

AGE_GROUPS = ["0-18", "19-29", "30-40", "41-50", "51-60", "61-70", "71+"]

GENDERS = ["Male", "Female", "Non-binary", "Prefer not to say"]

ILLNESSES = ["Diabetes", "Hypertension", "Asthma", "Arthritis",
             "Depression", "Anxiety", "COPD", "Heart Disease", "Epilepsy"]

ZIP_CODES = ["10115", "10117", "10119", "10178", "10179",
             "10243", "10245", "10247", "10249", "10315"]

FIRST_NAMES = ["James", "Maria", "john", "SARAH", "Ahmed", "lisa",
               "MICHAEL", "Fatima", "David", "anna", "Chris", "EMMA",
               "Daniel", "Sofia", "OLIVER", "julia", "thomas", "NINA",
               "Kevin", "laura"]

LAST_NAMES = ["smith", "MUELLER", "Khan", "Schmidt", "Jones", "WAGNER",
              "Brown", "Becker", "Wilson", "HOFFMANN", "Taylor", "Weber",
              "Anderson", "SCHULZ", "Martinez", "Fischer", "Garcia", "MEYER",
              "Robinson", "Wolf"]

# ── Helpers ─────────────────────────────────────────────────────────
def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def random_insurance():
    return f"INS-{random.randint(100000, 999999)}"

def random_appointment_datetime():
    base = datetime(2024, 1, 1)
    delta = timedelta(days=random.randint(0, 364),
                      hours=random.randint(8, 17),
                      minutes=random.choice([0, 15, 30, 45]))
    return (base + delta).strftime("%Y-%m-%d %H:%M")

def random_phone():
    """Messy phone formats"""
    formats = [
        f"+49-30-{random.randint(1000000, 9999999)}",
        f"030-{random.randint(1000000, 9999999)}",
        f"0{random.randint(100000000, 999999999)}",
        f"+49 30 {random.randint(100000, 999999)} {random.randint(10, 99)}",
    ]
    return random.choice(formats)

def random_illnesses():
    """Some patients have none, some have multiple"""
    count = random.choices([0, 1, 2, 3], weights=[30, 40, 20, 10])[0]
    return random.sample(ILLNESSES, count) if count > 0 else []

# ── Patient master list (100 patients) ──────────────────────────────
patients = []
for i in range(1, 101):
    patients.append({
        "patient_id": f"PAT-{str(i).zfill(3)}",
        "name": random_name(),
        "insurance_number": random_insurance(),
        "age_group": random.choice(AGE_GROUPS),
        "gender": random.choice(GENDERS),
        "zip_code": random.choice(ZIP_CODES),
        "blood_type": random.choice(BLOOD_TYPES),
        "long_term_illnesses": random_illnesses(),
    })

# ── Assign scenarios ─────────────────────────────────────────────────
# 4 emergency patients → no portal record
# 96 patients → portal record
emergency_ids = random.sample(range(100), 4)
portal_patients = [p for i, p in enumerate(patients) if i not in emergency_ids]
emergency_patients = [p for i, p in enumerate(patients) if i in emergency_ids]

# Among 96 portal patients: 10 no-show, 86 showed up
no_show_ids = set(random.sample(range(len(portal_patients)), 10))

# ── Generate portal_appointments.csv ────────────────────────────────
portal_rows = []
apt_counter = 1

for i, p in enumerate(portal_patients):
    status = "no_show" if i in no_show_ids else "showed_up"
    # Occasionally missing department (messy data)
    department = random.choice(DEPARTMENTS) if random.random() > 0.05 else ""
    portal_rows.append({
        "appointment_id": f"APT-{str(apt_counter).zfill(4)}",
        "name": p["name"],
        "department": department,
        "appointment_datetime": random_appointment_datetime(),
        "appointment_status": status,
        "registration_source": "portal",
    })
    apt_counter += 1

csv_path = "data/portal_appointments.csv"
with open(csv_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=portal_rows[0].keys())
    writer.writeheader()
    writer.writerows(portal_rows)

print(f"✅ portal_appointments.csv → {len(portal_rows)} rows")

# ── Generate patient_records.json ───────────────────────────────────
# 60 new patients get full reception record
# 40 existing patients → no reception record needed
new_patient_indices = random.sample(range(len(portal_patients)), 60)
new_patients = [portal_patients[i] for i in new_patient_indices]

reception_records = []

# New patients from portal
for i, p in enumerate(new_patients):
    apt_id = f"APT-{str(portal_patients.index(p) + 1).zfill(4)}"
    # Some missing emergency contacts (messy data)
    has_emergency_contact = random.random() > 0.15
    record = {
        "patient_id": p["patient_id"],
        "insurance_number": p["insurance_number"],
        "name": p["name"],
        "age_group": p["age_group"],
        "gender": p["gender"],
        "zip_code": p["zip_code"],
        "blood_type": p["blood_type"],
        "long_term_illnesses": p["long_term_illnesses"],
        "emergency_contact_name": random_name() if has_emergency_contact else None,
        "emergency_contact_phone": random_phone() if has_emergency_contact else None,
        "appointment_id": apt_id,
        "visit_type": "scheduled",
        "appointment_status": "showed_up",
        "registration_source": "portal_then_reception",
        "created_at": datetime(2024, random.randint(1, 12),
                               random.randint(1, 28)).strftime("%Y-%m-%d"),
    }
    reception_records.append(record)

# Emergency patients → no appointment
for p in emergency_patients:
    has_emergency_contact = random.random() > 0.15
    record = {
        "patient_id": p["patient_id"],
        "insurance_number": p["insurance_number"],
        "name": p["name"],
        "age_group": p["age_group"],
        "gender": p["gender"],
        "zip_code": p["zip_code"],
        "blood_type": p["blood_type"],
        "long_term_illnesses": p["long_term_illnesses"],
        "emergency_contact_name": random_name() if has_emergency_contact else None,
        "emergency_contact_phone": random_phone() if has_emergency_contact else None,
        "appointment_id": None,
        "visit_type": "emergency",
        "appointment_status": "no_appointment",
        "registration_source": "reception",
        "created_at": datetime(2024, random.randint(1, 12),
                               random.randint(1, 28)).strftime("%Y-%m-%d"),
    }
    reception_records.append(record)

json_path = "data/patient_records.json"
with open(json_path, "w") as f:
    json.dump(reception_records, f, indent=2)

print(f"✅ patient_records.json  → {len(reception_records)} records")

# ── Quick sanity check ───────────────────────────────────────────────
print("\n── Portal CSV breakdown ──")
statuses = [r["appointment_status"] for r in portal_rows]
print(f"  showed_up : {statuses.count('showed_up')}")
print(f"  no_show   : {statuses.count('no_show')}")

print("\n── Reception JSON breakdown ──")
visit_types = [r["visit_type"] for r in reception_records]
print(f"  scheduled : {visit_types.count('scheduled')}")
print(f"  emergency : {visit_types.count('emergency')}")

missing_contacts = sum(1 for r in reception_records if r["emergency_contact_name"] is None)
print(f"  missing emergency contact: {missing_contacts}")
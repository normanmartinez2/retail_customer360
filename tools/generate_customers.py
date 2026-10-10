"""Deterministic synthetic CRM export. Run from repository root."""
import csv, hashlib, json
from pathlib import Path
from datetime import date, timedelta

root = Path(__file__).resolve().parents[1]
out = root / 'data/crm'
out.mkdir(parents=True, exist_ok=True)
columns = ['customer_id','first_name','last_name','email','phone','date_of_birth','country','city','postal_code','customer_segment','marketing_consent','created_at','updated_at','source_system','notes']
names = ['Lucía','Mateo','Zoë','André','Sofía','Łukasz','Marta','Óscar','Chloé','Núria']
places = [('ES','Madrid','28001'),('ES','Barcelona','08001'),('FR','Paris','75001'),('PT','Lisboa','01000'),('DE','Berlin','10115')]
rows = []
for i in range(1,1201):
    country, city, postal = places[(i-1)%5]
    created = date(2024,1,1) + timedelta(days=i%365)
    rows.append([f'C{i:06d}',names[(i-1)%10],f'Apellido{i:04d}',f'customer{i:04d}@example.com',f'+3491{i:07d}',str(date(1970,1,1)+timedelta(days=i*11)),country,city,postal,['Standard','Silver','Gold'][i%3],['true','false'][i%2],f'{created}T09:00:00Z',f'{created+timedelta(days=30)}T10:00:00Z','CRM_SIMULATED','Cliente sintético'])
def change(start,stop,col,value):
    for i in range(start-1,stop): rows[i][columns.index(col)] = value
change(1,10,'customer_id','')
change(11,25,'email','')
change(26,37,'email','correo-sin-arroba')
for i in range(38,48): rows[i-1][3] = f'  CUSTOMER{i:04d}@EXAMPLE.COM  '
change(48,55,'first_name','  Lucía  ')
change(56,61,'date_of_birth','1980-02-30')
change(62,65,'date_of_birth','2099-01-01')
change(66,72,'country','es')
change(73,77,'country','XX')
change(78,83,'customer_segment','PlatinumLegacy')
change(84,89,'marketing_consent','YES')
change(90,93,'marketing_consent','unknown')
change(94,99,'updated_at','2023-01-01T00:00:00Z')
change(100,103,'updated_at','not-a-timestamp')
change(104,108,'phone','')
change(109,111,'notes','Prefiere "email", atención en español')
change(112,114,'notes','Primera línea\nSegunda línea: envío a domicilio')
change(115,115,'last_name',"O'Connor")
change(116,118,'city','')
change(119,122,'postal_code','00007')
# Ten exact replays and ten conflicting versions, separate from the defects above.
rows += [r.copy() for r in rows[199:209]]
for r in rows[299:309]:
    r = r.copy(); r[9] = 'Gold'; r[12] = '2025-06-01T10:00:00Z'; r[14] = 'Cambio de segmento'; rows.append(r)
path = out/'customers_lab02_v1.csv'
with path.open('w',encoding='utf-8',newline='') as f:
    writer = csv.writer(f,lineterminator='\n'); writer.writerow(columns); writer.writerows(rows)
manifest = {'dataset':'customers_lab02_v1','synthetic':True,'rows':len(rows),'columns':columns,'encoding':'UTF-8','delimiter':',','sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'expected':{'missing_customer_id':10,'missing_email':15,'invalid_nonempty_email':12,'duplicate_key_groups':20,'excess_rows_by_nonempty_key':20,'exact_duplicate_excess':10,'invalid_birth_dates':6,'future_birth_dates':4,'updated_before_created':6,'invalid_updated_at':4,'multiline_notes':3}}
(out/'customers_lab02_v1.manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(manifest,ensure_ascii=False))

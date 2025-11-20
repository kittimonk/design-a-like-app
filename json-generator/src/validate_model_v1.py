import json,sys,os
p = os.path.join(r"c:\Users\nirmal.k.chandak\json-automation\design-a-like-app\json-generator\generated_out","model_v1.json")
try:
    m = json.load(open(p, 'r', encoding='utf-8'))
except Exception as e:
    print(f"FAILED to parse JSON: {e}")
    sys.exit(2)
errs = []
if not isinstance(m.get('base_entity'), str) or not m['base_entity'].strip():
    errs.append('base_entity missing or invalid')
for k in ('joins','business_rules','derived','statics'):
    if k not in m or not isinstance(m[k], list):
        errs.append(f"{k} missing or not a list")
# collect targets
targets = []
for d in m.get('derived', []):
    t = d.get('target')
    if not t:
        errs.append('derived column with missing target')
    targets.append(('derived', t))
    if 'expression' not in d or (d.get('expression') or '') == '':
        errs.append(f"derived {t!r} has empty expression")
for s in m.get('statics', []):
    t = s.get('target')
    if not t:
        errs.append('static column with missing target')
    targets.append(('static', t))
    if 'expression' not in s or (s.get('expression') or '') == '':
        errs.append(f"static {t!r} has empty expression")
# duplicates
seen = set(); dups = []
for src,t in targets:
    if t is None: continue
    k = t.strip().lower()
    if k in seen:
        dups.append(t)
    else:
        seen.add(k)
# datatype checks
allowed_prefixes = ('STRING','BIGINT','INT','DATE','TIMESTAMP','DECIMAL','DOUBLE','FLOAT')
for item in (m.get('derived', []) + m.get('statics', [])):
    dt = (item.get('datatype') or '').upper()
    if dt == '':
        errs.append(f"missing datatype for target {item.get('target')}")
    elif not any(dt.startswith(x) for x in allowed_prefixes):
        errs.append(f"unrecognized datatype '{dt}' for target {item.get('target')}")
# final report
print('base_entity:', m.get('base_entity'))
print('joins:', len(m.get('joins', [])), 'derived:', len(m.get('derived', [])), 'statics:', len(m.get('statics', [])))
if dups:
    print('DUPLICATE TARGETS:', dups)
if errs:
    print('\nVALIDATION ERRORS:')
    for e in errs:
        print('-', e)
    sys.exit(2)
print('Validation OK')
sys.exit(0)

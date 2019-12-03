### Setup secrets

This secret is used by the database and the api as password.

```
apiVersion: v1
kind: Secret
metadata:
  name: odm2_db_password
type: Opaque
data:
  password: base64 encoded password (remember to use echo -n)
```
and
```
apiVersion: v1
kind: Secret
metadata:
  name: odm2_postgres_password
type: Opaque
data:
  password: base64 encoded password (remember to use echo -n)
```
make 4 secrets, 2 for nivates and 2 for nivaprod. Apply them using:
```
kubectl apply -f my_secret.yaml
```

# Datos

`raw/cloud_provider_challenge_dataset_v1.zip` es el dataset sintético que entregó la cátedra (1,9 MB comprimido, 13 MB descomprimido; SHA-256 `49bdc33693426add261750a0d4e6225d3fc5f9e334dc0689786f43f8d29d1141`). Trae las ocho fuentes bajo `datalake/landing/` y un README propio.

`make landing` extrae esas fuentes a `datalake/landing/` en la raíz del repo, las deja en solo lectura y las compara con `landing_manifest.csv`, que tiene el tamaño y el SHA-256 de los 127 archivos. `datalake/` no se versiona: se regenera desde el zip.

El dataset no tiene datos personales reales. Los emails de `users.csv` son del dominio `example.com`, y aun así se tratan como dato personal en Silver.

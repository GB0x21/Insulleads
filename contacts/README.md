# 📂 contacts/

Coloca aquí todos tus archivos `.csv` de contactos de GCs.
El sistema los detecta y carga **automáticamente** — no se necesita configuración adicional.

---

## ✅ Formatos de columnas reconocidos

| Tipo | Nombres de columna aceptados |
|------|------------------------------|
| **Nombre** | `Nombre`, `Name`, `Company`, `Empresa`, `Contractor`, `Business`, `GC`, `Contratista`, `Business Name`, `Contractor Name` |
| **Teléfono** | `Numero`, `Number`, `Phone`, `Telefono`, `Tel`, `Celular`, `Mobile`, `Cell`, `Phone Number`, `Movil` |
| **Email** | `Email`, `Correo`, `Mail`, `E-mail`, `Email Address`, `Correo_Electronico` |

## 📋 Ejemplos válidos

```csv
Nombre,Numero
ABC CONSTRUCTION,+15105663424
```

```csv
Company,Phone,Email
ABC CONSTRUCTION,(510) 566-3424,info@abc.com
```

```csv
Name,Email
SMITH BUILDERS,smith@builders.com
```

```csv
Business Name,Phone Number,Email Address
ACE CONTRACTORS INC,510-234-5678,ace@contractors.com
```

## 🔧 Notas

- El separador puede ser `,` o `;` — se detecta automáticamente.
- Un mismo GC en varios archivos → se fusionan los datos.
- Archivos con solo teléfono, solo email, o ambos son válidos.
- El `.gitignore` está configurado para **incluir** los CSVs en el repo.
  Si no quieres subirlos, descomenta la línea `contacts/*.csv` en `.gitignore`.

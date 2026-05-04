# CLAUDE.md — Insulleads Development Guidelines

Guías de codificación basadas en los principios de Andrej Karpathy para reducir
errores costosos. Aplican a Claude Code, Cursor y cualquier asistente de IA que
trabaje en este repositorio.

---

## 1. Think Before Coding — Piensa Antes de Implementar

**Antes de escribir una sola línea de código:**

- Nombra explícitamente cualquier ambigüedad. Si algo no está claro, pregunta.
- Verifica que entiendes la fuente de datos (Socrata, CKAN, SeeClickFix, etc.)
  antes de tocar el `field_map` — los campos varían por ciudad y dataset.
- Cuando modifiques un agente existente, lee primero el `ROOT CAUSE ANALYSIS`
  del docstring para entender qué problemas ya fueron resueltos.
- Ante la duda sobre un campo de API, haz una request de prueba antes de asumir.

**Señales de alarma:**
- "Creo que el campo se llama X" → Verifica primero.
- "Seguramente funciona igual que el otro agente" → Léelo antes.

---

## 2. Simplicity First — Código Mínimo

**Escribe solo el código que resuelve el problema actual.**

- Sin abstracciones para un solo caso de uso.
- Sin features que no se pidieron.
- Si la solución puede ser 30 líneas en vez de 150, reescríbela.
- Tres líneas similares son mejores que una abstracción prematura.

**Para los agentes Insulleads:**
- Los agentes heredan de `BaseAgent`. Implementa solo `fetch_leads()` y `notify()`.
- No agregues enrichment en un agente nuevo hasta que el fetch básico funcione.
- No copies código de otro agente sin entender qué hace cada línea.

**Patrones a evitar:**
```python
# ❌ Over-engineered
class FlexibleDataFetcher(Generic[T], metaclass=ABCMeta):
    ...

# ✅ Directo
def _fetch_socrata(url, params, timeout) -> list:
    resp = requests.get(url, params=params, timeout=timeout)
    return resp.json()
```

---

## 3. Surgical Changes — Solo lo Necesario

**Al modificar código existente:**

- Edita solo lo que tu tarea requiere.
- No refactorices código adyacente que funciona.
- No elimines imports o funciones que no rompiste.
- No "limpies" mientras arreglas — son dos tareas separadas.

**Para los agentes:**
- Si cambias el `field_map` de SF, no toques el de SJ.
- Si agregas una ciudad nueva a `rodents_agent`, usa `_scf()` — no copies bloques.
- Si corriges un bug en `_fetch_source()`, no refactorices el loop principal.

**Test de "cambio mínimo":**
- ¿El diff incluye líneas que no tienen relación con la tarea? Si sí, hay que revertirlas.

---

## 4. Goal-Driven Execution — Criterios Verificables Primero

**Antes de implementar, define el éxito:**

```
Tarea: "Agregar ciudad Vallejo al rodents_agent"

Criterio de éxito:
  ✓ python main.py --run rodents no lanza excepción
  ✓ El agente fetcha > 0 leads de Vallejo en los últimos 2 meses
  ✓ Los tests existentes siguen pasando: pytest outreach/tests/

NO es criterio de éxito:
  ✗ "Se ve bien en el código"
  ✗ "Parece que funciona"
```

**Para bugs:** Escribe primero un test que reproduzca el bug, luego corrígelo.

---

## 5. Reglas Específicas de Insulleads

### Agentes

- **Un agente = una fuente de datos**. No fusiones dos fuentes en un agente.
- Todo nuevo agente hereda de `BaseAgent` y llama `send_batch()`, no `send_if_new()`.
- Los agentes no importan entre sí. Si necesitas lógica compartida, ponla en `utils/`.
- El `agent_key` debe ser único y consistente con el registro en `main.py`.

### Fuentes de Datos

- Los engines reconocidos son: `socrata`, `ckan`, `ckan_sql`, `seeclickfix`.
- Para añadir una ciudad SeeClickFix al `rodents_agent`, usa `_scf(city, slug)`.
- Para añadir una ciudad nueva al `permits_agent`, replica la estructura del
  `field_map` más cercano — los campos varían por portal municipal.
- Siempre pon `_skip_if_no_data: True` en fuentes experimentales.

### Base de Datos

- `init_db()` en `utils/db.py` inicializa todas las tablas. Llámala siempre antes de usar `is_sent()` o `mark_sent()`.
- Las tablas de memoria (`memories`, `agent_summaries`) y outcomes (`lead_outcomes`) se crean automáticamente en `init_db()`.
- No escribas SQL directo en los agentes. Usa las utilidades de `utils/`.

### Scoring y Memoria

- `score_lead_with_memory()` en vez de `score_lead()` cuando el agente está integrado con `send_batch()` (ya se hace automáticamente en `BaseAgent`).
- Para registrar un outcome desde el CRM, el endpoint `label_lead` en `crm/views.py` lo hace automáticamente.
- La memoria se comprime automáticamente cada `MEMORY_COMPRESS_TRIGGER` observaciones.

### Tests

- Los tests viven en `outreach/tests/`.
- Cada test es unitario: sin red, sin Django ORM, sin Telegram.
- Para tests de agentes: mockea `requests.get` con `responses` o `unittest.mock`.
- Para tests de utilidades de DB: usa `DB_PATH=/tmp/test_X.db` en el env.

---

## 6. Lo que NO hacer

| ❌ Prohibido | ✅ Alternativa |
|---|---|
| Copiar bloques seeclickfix | Usar `_scf(city, slug)` |
| `import *` en agentes | Imports explícitos |
| `print()` para debugging | `logger.debug()` |
| Hard-codear API keys | Variables en `.env` |
| Modificar `main.py` para un agente puntual | Usar `--run agente` |
| Agregar `try/except Exception: pass` silencioso | Log el error con `logger.debug()` |
| Reescribir un agente completo cuando el bug está en 2 líneas | Cambio quirúrgico |

---

## 7. Comandos Útiles

```bash
# Ejecutar un agente puntualmente (sin scheduler)
python main.py --run permits

# Ver estado del sistema
python main.py --health
python main.py --stats

# Tests
pytest outreach/tests/ -v
pytest outreach/tests/test_permit_filters.py -v  # tests específicos

# Django CRM
python manage.py runserver       # CRM en :8000
python manage.py createsuperuser # Crear usuario admin
python manage.py setup_crm       # Bootstrap campaña por defecto
```

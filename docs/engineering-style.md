# Engineering Style

## Cohesive Classes

Prefer small, purpose-named classes when code owns a cohesive responsibility.

- API route groups should be classes such as `NotificationRoutes` or `UserRoutes`.
- API route methods should use `@route(...)` so path, method, response model, and status code stay beside the handler while the class still owns one `APIRouter`.
- Stateful helpers such as cursor encoding/decoding should be classes such as `WebNotificationCursorCodec`.
- Test factories, token builders, and seed-data helpers should live in file-local helper classes such as `NotificationApiFixtures`, not as loose top-level helper functions.

Top-level functions are acceptable for pytest fixtures only. Actual `test_*` cases should be grouped into purpose-named `Test...` classes so related behavior stays under one cohesive unit. For production code, use standalone functions only when they are simple stateless transformations and do not form a group of related operations. If a file starts collecting related helper functions, introduce a cohesive class instead.

## Domain Types

Domain-like data carriers belong under `backend.domain`. This includes enums and dataclasses that represent business concepts, value objects, read models, or use-case result DTOs.

- Put business enums in `backend.domain.enums`.
- Put domain value objects in `backend.domain.value_objects`.
- Put read models in `backend.domain.read_models`.
- Put use-case result DTOs in `backend.domain.results`.
- `api`, `core`, `db`, `services`, and `workers` should import these domain types instead of redefining equivalent data carriers locally.
- Technical helper metadata that only belongs to one layer, such as route-registration metadata, can stay in that layer as a regular focused class.
- SQLAlchemy models, declarative bases, and timestamp mixins are database mapping concerns only. They must not define domain dataclasses or enums.

## Tests

Test files should keep setup helpers organized by responsibility:

- `*Fixtures` for model factories, service construction, and seed data.
- `*Tokens` for JWT or auth-token builders.
- Test classes should group related scenarios, for example `TestNotificationCreationService`, `TestRetryService`, or `TestUserNotificationApi`.
- Test methods should call setup helpers through the class name so the test data source is explicit.

This keeps test files readable as they grow and prevents a module from becoming a bag of unrelated helper functions.

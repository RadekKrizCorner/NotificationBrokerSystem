from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from jinja2 import (
    Environment,
    FileSystemLoader,
    StrictUndefined,
    TemplateError,
)


class EmailTemplateError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RenderedEmail:
    subject: str
    plain_body: str
    html_body: str


class EmailTemplateRenderer:
    _subject_template = "subject.j2"
    _plain_template = "plain.txt.j2"
    _html_template = "html.html.j2"

    def __init__(self, *, template_directory: Path) -> None:
        self._environment = Environment(
            loader=FileSystemLoader(str(template_directory)),
            autoescape=lambda template_name: template_name == self._html_template,
            undefined=StrictUndefined,
            auto_reload=False,
        )

    @classmethod
    def default(cls) -> EmailTemplateRenderer:
        return cls(template_directory=Path(__file__).with_name("templates"))

    def render(self, *, context: Mapping[str, object]) -> RenderedEmail:
        try:
            subject = self._environment.get_template(self._subject_template).render(context)
            plain_body = self._environment.get_template(self._plain_template).render(context)
            html_body = self._environment.get_template(self._html_template).render(context)
        except (OSError, TemplateError) as exc:
            raise EmailTemplateError("email template rendering failed") from exc

        normalized_subject = " ".join(subject.split())[:200]
        if not normalized_subject:
            raise EmailTemplateError("email template rendered an empty subject")
        if not plain_body.strip() or not html_body.strip():
            raise EmailTemplateError("email template rendered an empty body")

        return RenderedEmail(
            subject=normalized_subject,
            plain_body=plain_body,
            html_body=html_body,
        )

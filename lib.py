from __future__ import annotations
import dataclasses
import inspect
from lxml import etree


def to_xml(
    obj,
    root_name: str | None = None,
    *,
    include_types: bool = False,
    attrs_as_xml_attrs: bool = True,
    include_private: bool = False,  # include attributes starting with '_'
    include_properties: bool = False,  # include @property values
    max_depth: int | None = None,  # cap recursion depth
) -> etree.Element:
    """
    Convert an arbitrary Python object (esp. class instances) to pretty XML (lxml).
    """

    def sanitize_tag(tag: str) -> str:
        # XML names must start with letter/_ and contain letters, digits, hyphens, underscores, periods
        if not tag:
            tag = "item"
        safe = []
        for i, ch in enumerate(tag):
            ok = ch.isalnum() or ch in "._-"
            if i == 0 and (ch.isdigit() or not ok):
                safe.append("_")
            safe.append(ch if ok else "_")
        # ensure first char not digit
        if safe[0].isdigit():
            safe.insert(0, "_")
        return "".join(safe)

    def is_primitive(x):
        return isinstance(x, (str, int, float, bool, type(None)))

    def get_attrs(x):
        # dataclass
        if dataclasses.is_dataclass(x):
            return dataclasses.asdict(x)

        # pydantic v2/v1
        for method in ("model_dump", "dict"):
            fn = getattr(x, method, None)
            if callable(fn):
                try:
                    return fn()
                except Exception:
                    pass

        items = {}
        # __dict__
        d = getattr(x, "__dict__", None)
        if isinstance(d, dict):
            items.update(d)

        # __slots__
        slots = getattr(x, "__slots__", None)
        if slots:
            for s in slots:
                if isinstance(s, str) and hasattr(x, s):
                    try:
                        items[s] = getattr(x, s)
                    except Exception:
                        pass

        # properties
        if include_properties:
            for name, member in inspect.getmembers(
                type(x), lambda m: isinstance(m, property)
            ):
                try:
                    items[name] = getattr(x, name)
                except Exception:
                    pass

        if not include_private:
            items = {k: v for k, v in items.items() if not k.startswith("_")}

        return items

    visited = set()
    depth = 0

    def build(parent, value, name_hint: str | None = None, *, _is_root=False):
        nonlocal depth

        if max_depth is not None and depth > max_depth:
            elem = (
                parent
                if _is_root
                else etree.SubElement(parent, sanitize_tag(name_hint or "value"))
            )
            elem.text = "<max_depth_reached/>"
            return

        def mark_and_check(x):
            if is_primitive(x):
                return False
            oid = id(x)
            if oid in visited:
                return True
            visited.add(oid)
            return False

        # when _is_root=True, operate on 'parent' directly (no extra wrapper)
        target_elem = parent if _is_root else None

        # primitives
        if is_primitive(value):
            if _is_root:
                if include_types:
                    parent.set("type", type(value).__name__)
                parent.text = "" if value is None else str(value)
            else:
                tag = sanitize_tag(name_hint or "value")
                elem = etree.SubElement(parent, tag)
                if include_types:
                    elem.set("type", type(value).__name__)
                elem.text = "" if value is None else str(value)
            return

        # cycle guard
        if mark_and_check(value):
            tag = sanitize_tag(
                name_hint or getattr(value, "__class__", type(value)).__name__
            )
            elem = (
                target_elem is not None
                and len(target_elem)
                or etree.SubElement(parent, tag)
            )
            elem.set("ref", "cyclic")
            return

        # dict
        if isinstance(value, dict):
            tag = sanitize_tag(name_hint or "dict")
            elem = (
                target_elem is not None
                and len(target_elem)
                or etree.SubElement(parent, tag)
            )
            if include_types:
                elem.set("type", "dict")
            depth += 1
            try:
                for k, v in value.items():
                    build(elem, v, str(k))
            finally:
                depth -= 1
            return

        # sequence/set
        if isinstance(value, (list, tuple, set, frozenset)):
            tag = sanitize_tag(name_hint or type(value).__name__)
            elem = (
                target_elem is not None
                and len(target_elem)
                or etree.SubElement(parent, tag)
            )
            if include_types:
                elem.set("type", type(value).__name__)
            depth += 1
            try:
                for item in value:
                    build(elem, item, "item")
            finally:
                depth -= 1
            return

        # object with attributes
        attrs = get_attrs(value)
        tag = sanitize_tag(name_hint or value.__class__.__name__)
        elem = (
            target_elem is not None
            and len(target_elem)
            or etree.SubElement(parent, tag)
        )
        if include_types:
            elem.set("type", type(value).__name__)

        # choose attributes vs child elements
        prim_fields = {k: v for k, v in attrs.items() if is_primitive(v)}
        comp_fields = {k: v for k, v in attrs.items() if not is_primitive(v)}

        if attrs_as_xml_attrs:
            for k, v in prim_fields.items():
                elem.set(sanitize_tag(k), "" if v is None else str(v))
        else:
            comp_fields = attrs  # everything becomes child elements

        if comp_fields or (not attrs_as_xml_attrs and prim_fields):
            depth += 1
            try:
                for k, v in (
                    comp_fields.items() if attrs_as_xml_attrs else attrs.items()
                ):
                    build(elem, v, k)
            finally:
                depth -= 1

    # build root (single wrapper)
    root_tag = sanitize_tag(
        root_name or (obj.__class__.__name__ if not is_primitive(obj) else "root")
    )
    root = etree.Element(root_tag)
    build(root, obj, _is_root=True)

    return root

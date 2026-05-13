from django import template

register = template.Library()


@register.filter
def split(value, arg):
    return value.split(arg)


@register.filter
def get_item(lst, index):
    try:
        return lst[int(index)]
    except (IndexError, TypeError, ValueError):
        return ""


@register.filter
def symbol_color_style(symbol):
    """Return an inline CSS background gradient based on symbol hash."""
    gradients = [
        "linear-gradient(135deg,#3b82f6,#1d4ed8)",
        "linear-gradient(135deg,#8b5cf6,#6d28d9)",
        "linear-gradient(135deg,#10b981,#065f46)",
        "linear-gradient(135deg,#f59e0b,#b45309)",
        "linear-gradient(135deg,#f43f5e,#be123c)",
        "linear-gradient(135deg,#06b6d4,#0e7490)",
        "linear-gradient(135deg,#6366f1,#4338ca)",
        "linear-gradient(135deg,#f97316,#c2410c)",
    ]
    h = sum(ord(c) for c in (symbol or ""))
    return f"background:{gradients[h % len(gradients)]}"

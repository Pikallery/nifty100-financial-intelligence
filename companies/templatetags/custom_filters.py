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

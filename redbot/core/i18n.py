import re
from pathlib import Path

__all__ = ['get_locale', 'set_locale', 'reload_locales', 'CogI18n']

_current_locale = 'en_us'

WAITING_FOR_MSGID = 1
IN_MSGID = 2
WAITING_FOR_MSGSTR = 3
IN_MSGSTR = 4

MSGID = 'msgid "'
MSGSTR = 'msgstr "'

_i18n_cogs = {}


def get_locale():
    return _current_locale


def set_locale(locale):
    global _current_locale
    _current_locale = locale
    reload_locales()


def reload_locales():
    for cog_name, i18n in _i18n_cogs.items():
        i18n.load_translations()


def _parse(translation_file):
    """
    Custom gettext parsing of translation files. All credit for this code goes
    to ProgVal/Valentin Lorentz and the Limnoria project.

    https://github.com/ProgVal/Limnoria/blob/master/src/i18n.py

    :param translation_file:
        An open file-like object containing translations.
    :return:
        A set of 2-tuples containing the original string and the translated version.
    """
    step = WAITING_FOR_MSGID
    translations = set()
    for line in translation_file:
        line = line[0:-1]  # Remove the ending \n
        line = line

        if line.startswith(MSGID):
            # Don't check if step is WAITING_FOR_MSGID
            untranslated = ''
            translated = ''
            data = line[len(MSGID):-1]
            if len(data) == 0:  # Multiline mode
                step = IN_MSGID
            else:
                untranslated += data
                step = WAITING_FOR_MSGSTR

        elif step is IN_MSGID and line.startswith('"') and \
                line.endswith('"'):
            untranslated += line[1:-1]
        elif step is IN_MSGID and untranslated == '':  # Empty MSGID
            step = WAITING_FOR_MSGID
        elif step is IN_MSGID:  # the MSGID is finished
            step = WAITING_FOR_MSGSTR

        if step is WAITING_FOR_MSGSTR and line.startswith(MSGSTR):
            data = line[len(MSGSTR):-1]
            if len(data) == 0:  # Multiline mode
                step = IN_MSGSTR
            else:
                translations |= {(untranslated, data)}
                step = WAITING_FOR_MSGID

        elif step is IN_MSGSTR and line.startswith('"') and \
                line.endswith('"'):
            translated += line[1:-1]
        elif step is IN_MSGSTR:  # the MSGSTR is finished
            step = WAITING_FOR_MSGID
            if translated == '':
                translated = untranslated
            translations |= {(untranslated, translated)}
    if step is IN_MSGSTR:
        if translated == '':
            translated = untranslated
        translations |= {(untranslated, translated)}
    return translations


def _normalize(string, remove_newline=False):
    """
    String normalization.

    All credit for this code goes
    to ProgVal/Valentin Lorentz and the Limnoria project.

    https://github.com/ProgVal/Limnoria/blob/master/src/i18n.py

    :param string:
    :param remove_newline:
    :return:
    """
    def normalize_whitespace(s):
        """Normalizes the whitespace in a string; \s+ becomes one space."""
        if not s:
            return str(s)  # not the same reference
        starts_with_space = (s[0] in ' \n\t\r')
        ends_with_space = (s[-1] in ' \n\t\r')
        if remove_newline:
            newline_re = re.compile('[\r\n]+')
            s = ' '.join(filter(bool, newline_re.split(s)))
        s = ' '.join(filter(bool, s.split('\t')))
        s = ' '.join(filter(bool, s.split(' ')))
        if starts_with_space:
            s = ' ' + s
        if ends_with_space:
            s += ' '
        return s

    string = string.replace('\\n\\n', '\n\n')
    string = string.replace('\\n', ' ')
    string = string.replace('\\"', '"')
    string = string.replace("\'", "'")
    string = normalize_whitespace(string)
    string = string.strip('\n')
    string = string.strip('\t')
    return string


def get_locale_path(cog_folder: Path, extension: str) -> Path:
    """
    Gets the folder path containing localization files.

    :param Path cog_folder:
        The cog folder that we want localizations for.
    :param str extension:
        Extension of localization files.
    :return:
        Path of possible localization file, it may not exist.
    """
    return cog_folder / 'locales' / "{}.{}".format(get_locale(), extension)


class CogI18n:
    def __init__(self, name, file_location):
        """
        Initializes the internationalization object for a given cog.

        :param name: Your cog name.
        :param file_location:
            This should always be ``__file__`` otherwise your localizations
            will not load.
        """
        self.cog_folder = Path(file_location).resolve().parent
        self.cog_name = name
        self.translations = {}

        _i18n_cogs.update({self.cog_name: self})

        self.load_translations()

    def __call__(self, untranslated: str):
        normalized_untranslated = _normalize(untranslated, True)
        try:
            return self.translations[normalized_untranslated]
        except KeyError:
            return untranslated

    def load_translations(self):
        """
        Loads the current translations for this cog.
        """
        self.translations = {}
        translation_file = None
        locale_path = get_locale_path(self.cog_folder, 'po')
        try:

            try:
                translation_file = locale_path.open('ru')
            except ValueError:  # We are using Windows
                translation_file = locale_path.open('r')
            self._parse(translation_file)
        except (IOError, FileNotFoundError):  # The translation is unavailable
            pass
        finally:
            if translation_file is not None:
                translation_file.close()

    def _parse(self, translation_file):
        self.translations = {}
        for translation in _parse(translation_file):
            self._add_translation(*translation)

    def _add_translation(self, untranslated, translated):
        untranslated = _normalize(untranslated, True)
        translated = _normalize(translated)
        if translated:
            self.translations.update({untranslated: translated})

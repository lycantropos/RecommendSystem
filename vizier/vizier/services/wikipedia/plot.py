import wikipedia

PLOT_SECTION_NAMES = ['Plot', 'PlotEdit', 'Synopsis',
                      'Plot summary', 'Plot synopsis']


async def parse_plot_content(link: str) -> str:
    page = wikipedia.page(link, auto_suggest=False)
    plot_sections = filter(None,
                           (page.section(section_title)
                            for section_title in PLOT_SECTION_NAMES))
    return ''.join(plot_sections).replace('\n', '')

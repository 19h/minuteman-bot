pub enum HeaderItem {
    Title {
        label: String,
    },
    Link {
        label: String,
        url: Option<String>,
    },
}

impl From<HeaderItem> for String {
    fn from(item: HeaderItem) -> Self {
        match item {
            HeaderItem::Title { ref label } =>
                format!("<span class=\"title\">{}</span>", label),
            HeaderItem::Link { ref label, ref url } =>
                match url {
                    Some(ref url) =>
                        format!("<a href=\"{}\">{}</a>", url, label),
                    None =>
                        format!("<span class=\"nolink\">{} (none)</span>", label),
                }
        }
    }
}

pub struct HeaderBar {
    items: Vec<HeaderItem>,
}

impl HeaderBar {
    pub fn new() -> Self {
        HeaderBar {
            items: vec!(),
        }
    }

    pub fn with_item(
        mut self,
        item: HeaderItem,
    ) -> Self {
        self.items.push(item);

        self
    }

    pub fn with_title(
        mut self,
        label: String,
    ) -> Self {
        self.items.push(
            HeaderItem::Title {
                label,
            },
        );

        self
    }

    pub fn with_link(
        mut self,
        label: &str,
        url: Option<String>,
    ) -> Self {
        self.items.push(
            HeaderItem::Link {
                label: label.to_string(),
                url,
            },
        );

        self
    }
}

impl From<HeaderBar> for String {
    fn from(item: HeaderBar) -> Self {
        format!(
            "<div class=\"navigation\">{}</div>",
            item.items
                .into_iter()
                .map(|item|
                    item.into()
                )
                .collect::<Vec<String>>()
                .join(" | "),
        )
    }
}

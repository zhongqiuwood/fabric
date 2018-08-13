package litekfk

type Reader struct {
	*reader
}

type Client struct {
	client
	bind *topicUnit
}

func (c *Client) UnReg() {
	c.client.UnReg(c.bind)
}

func (c *Client) Read(beginPos int) (Reader, error) {
	rd, err := c.client.Read(c.bind, beginPos)
	if err != nil {
		return Reader{}, err
	}

	return Reader{rd}, nil
}

type Topic struct {
	*topicUnit
}

func (t Topic) NewClient() *Client {
	return &Client{t.Clients().AssignClient(), t.topicUnit}
}

func (t Topic) Watcher() TopicWatcher {
	return TopicWatcher{t.topicUnit}
}

func CreateTopic(conf *topicConfiguration) Topic {

	return Topic{InitTopic(conf)}
}

//obtain a topic wather, access some members without locking
type TopicWatcher struct {
	*topicUnit
}

func (w TopicWatcher) GetStart() *readerPos {
	return &readerPos{Element: w.start.Element}
}

func (w TopicWatcher) GetTail() *readerPos {
	return &readerPos{
		Element: w.data.Back(),
		logpos:  w.data.Back().Value.(*batch).wriPos,
	}
}

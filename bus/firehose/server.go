package kafkafirehose

type KafkaFirehose struct {
}

type Args struct {
}

func New(args *Args) (*KafkaFirehose, error) {
	kf := KafkaFirehose{}

	return &kf, nil
}

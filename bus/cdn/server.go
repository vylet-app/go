package kafkacdn

type KafkaCdn struct {
}

type Args struct {
}

func New(args *Args) (*KafkaCdn, error) {
	kf := KafkaCdn{}

	return &kf, nil
}
